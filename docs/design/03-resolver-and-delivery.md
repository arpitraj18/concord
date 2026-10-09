# Phase 3 — Uncertainty resolver and exactly-once delivery

## Problem

Phase 2 ended with two numbers: about 8–10% of payments end **behind** the truth (their last
webhooks were dropped) and about 1% end **ahead** of it (a lie that fits a legal path). No system
that only listens to webhooks can fix either. And everything Concord decides must reach the merchant
exactly once in effect, even though every hop (gateway → Concord → merchant) is at-least-once.

```
Resolver:  EventBridge Scheduler ─> sweep ─> candidates (GSI1 by state+age, sparse GSI2 for flags)
                                       └─> status API (circuit breaker + rate limiter per gateway)
                                       └─> same conditional engine write as webhooks (apply / verify / correct)
                                       └─> merchant order adapter ─> capture or release long-authorized payments

Delivery:  engine commit = STATE + EVT + OUTBOX in one transaction ─> DynamoDB Streams (CDC)
              ─> delivery worker (per-payment lanes, retries with jitter, DLQ) ─> merchant
              ─> merchant: processed_events PK + version guard + effects only on status change
```

## Modules

| Module | What |
|---|---|
| `resolver` | `Resolver` sweep, `HttpGatewayClient` (Resilience4j circuit breaker + rate limiter per gateway), `MerchantOrders` adapter, resolver bookkeeping (memory / DynamoDB), `ResolverLambda` for EventBridge Scheduler |
| `delivery` | `DynamoDbStreamsOutboxReader` (CDC), `DeliveryWorker`, `DeliverySigner`, dead-letter sinks |
| `sample-merchant` | Spring Boot + PostgreSQL shop: orders, `processed_events`, `effects`; shares no code with Concord |
| `local-runner` | Everything in one process for local runs and `docker compose` |
| `system-tests` | The done-when test, everything wired together |

Engine additions: `register` (payments known before any webhook), `verify` (status API agrees: clear
a flag), `correct` (status API disagrees, twice: audited correction), status-API mappings in the
same versioned JSON as webhook mappings, a sparse GSI2, outbox entries that carry a snapshot of
order id and amount, and a `kind` (TRANSITION / CORRECTION).

## Design decisions

### 1. Exactly-once delivery is impossible; exactly-once *effect* is not

Not over a network: a request can succeed and its response be lost. So each hop is at-least-once and
the receiver is idempotent:

- Outbox → stream: the OUTBOX item is written in the **same transaction** as the state change, so it
  exists if and only if the change happened (no dual-write problem). Streams keep each payment's
  changes in order.
- Stream → worker: positions are kept in memory; after a restart the reader starts again from the
  oldest stream record (24 h), so events are re-delivered. Safe by design.
- Worker → merchant: retried until 2xx, with exponential backoff and **full jitter** (a merchant coming
  back from an outage is not hit by a synchronised burst). One lane per payment, one delivery in flight
  per lane: a merchant never sees REFUNDED before CAPTURED. After the last retry, a dead-letter queue.
- Merchant: in **one PostgreSQL transaction**: `INSERT INTO processed_events ... ON CONFLICT DO
  NOTHING` (the primary key *is* the dedup; concurrent copies serialise on it), `SELECT ... FOR UPDATE`
  on the order, ignore versions older than the last applied, and emit a side effect only when the
  order's status actually changes (CAPTURED then SETTLED ships once).

Idempotency key = `gateway#paymentId#state#version`, from the plan.

### 2. The resolver: source of truth, applied through the same write as webhooks

The status API is the truth. Everything the resolver learns goes through the engine's conditional
write, so **a webhook and a status-API result racing for one payment need no special handling**: the
first commit wins, the second is re-decided against the new state and becomes a no-op. A 50-round race
test proves exactly one CAPTURED transition every time.

What a sweep looks at, and what it does:

| Candidate | Found how | Status API says… | Action |
|---|---|---|---|
| Flagged (contradiction) | sparse GSI2 `VERIFY` | same as held | `verify`: clear flag |
| CREATED / PENDING / UNKNOWN / AUTHORIZED for > `checkAfter` | GSI1 state + age | ahead | apply (a webhook was lost) |
| Any final state, once, `verifyFinalAfter` after reaching it | GSI1 time window | behind / other branch | correct, after a second look |
| CAPTURED on gateways that settle | GSI1 | SETTLED | apply |
| AUTHORIZED for > `actOnAuthorizedAfter` | — | still AUTHORIZED | ask the merchant: capture or release |
| anything | — | no answer | backoff + jitter; PENDING → UNKNOWN; park after `parkAfter` |

### 3. Corrections: the only way back, and never on one observation

Phase 2 proved webhooks alone cannot avoid holding a wrong state sometimes. Correcting it means
moving a payment outside the normal transitions (CAPTURED → AUTHORIZED). That is allowed only:

- from the status API, **observed twice**, at least `confirmDelay` apart, with the payment unchanged in
  between (status APIs can lag behind webhooks; a correction based on a stale read would be worse than
  none);
- conditional on the version the observations were made against;
- recorded as `CORRECTED` in the history, and delivered to the merchant as a `CORRECTION` event (the
  sample merchant reacts with `STOP_FULFILMENT`).

So the Phase 2 invariant becomes: *webhooks never move a payment backwards; only audited,
twice-confirmed corrections from the source of truth can.*

### 4. Verify every final state once

A lie that fits a legal path (a fake "captured" for a payment that was only authorized) looks exactly
like the truth. Phase 2 measured ~1% of payments like that. The fix is cheap and common in real payment
systems: check each payment's final state once, shortly after it is reached (one status call per
payment, skipped if that exact version was already confirmed). The query is a **time window**
(`gsi1sk BETWEEN`), not "older than", so payments already confirmed do not fill every batch forever.

### 5. Payments are registered before their first webhook

If every webhook of a payment is lost, Concord would never know it existed. So the merchant registers
each payment when it creates the order (`POST /payments/register`, done by the sample merchant on
order creation). The payment starts in CREATED and the resolver finds it like any other.

### 6. Timeouts mean "uncertain", not "failed"

A PENDING payment whose status API stays unreachable for `unknownAfter` becomes **UNKNOWN**, and the
merchant is told. UNKNOWN maps to "awaiting payment" at the merchant: do not cancel, do not ship. When
the status API answers, UNKNOWN moves to the confirmed state. Backoff is capped so the UNKNOWN
deadline is never missed because of jitter.

### 7. Polling politely

- Exponential backoff with full jitter per payment (thundering herd).
- One **rate limiter per gateway**: a call without a permit is not queued, it is rescheduled.
- One **circuit breaker per gateway**: after half the recent calls fail, stop calling for a while;
  404 is an answer, not a failure. One gateway's outage never slows the others.
- Parking is a **time budget** (`parkAfter`, default 24 h), not an attempt count. Parked payments go
  to sparse GSI2 `PARKED`: the human review queue. The config refuses a budget shorter than the
  UNKNOWN or capture thresholds.
- Resolver bookkeeping lives in a separate item (`SK=RESOLVER`), so scheduling a re-check never bumps
  the payment's version or conflicts with a webhook.

### 8. Authorized but never captured

The money is held on the customer's card, nobody captured it, and gateways auto-release only after
days. After `actOnAuthorizedAfter`, the resolver asks the merchant (through the `MerchantOrders`
adapter, never its database): order still valid → **capture** (save the sale); order cancelled →
**release** (money back in minutes). Once per payment, and the result goes through the normal engine
path. The simulator gained capture/void endpoints that rewrite the payment's future (planned webhooks
for steps that no longer happen are suppressed).

## Done when — met

> With webhooks dropped at a high rate, every payment still reaches its correct final state, and the
> merchant's processed-keys table shows zero duplicate effects.

`ExactlyOnceUnderHeavyLossTest`, for each gateway: 300 payments; **50% of webhooks dropped**, 30%
duplicated (20% of those with fresh ids), up to 4 s reordering, 5% contradictions; status API with 10%
random errors plus an outage window; half the stuck-authorized orders cancelled by the merchant.
Everything real except the clock speed: HTTP between all parts, DynamoDB transactions and Streams
(DynamoDB Local), PostgreSQL 17 (embedded binaries).

- Every payment's Concord state equals the simulator's final truth (after any capture / release), no
  payment is left flagged, and the merchant's order shows the same payment state.
- `duplicateEffects = 0` (no event caused two effects) and `ordersFulfilledWrongly = 0` (no order
  shipped twice).
- Then the delivery worker "crashes and restarts", re-reading the stream from the start: the merchant
  receives **every event a second time**, absorbs all of them, and its effects do not change.

Sample run:

```
gateway A: 300 payments; 724 webhooks planned, 376 dropped, 11 lies; resolver applied 191,
           corrected 8, captured 15, released 16; delivery 491 delivered, 0 dead-lettered;
           merchant: 982 deliveries received, 491 duplicates absorbed, 0 duplicate effects
```

The same flow was also run as separate processes (simulator, `local-runner`, merchant on a standalone
PostgreSQL server): all 60 payments converged to the truth within 10 s, and a stuck-authorized payment
was captured after the merchant confirmed its order.

## Running it

```bash
docker compose up --build        # postgres, merchant, concord (local-runner), gateways A/B/C
```

See README "Try it". `RESOLVER_PROFILE=demo` uses second-scale timings.

## Known gaps

- Stream positions are in memory (re-delivery after restart is safe but wasteful); a Lambda event
  source mapping on the stream would checkpoint for us.
- ECS services for the state worker and delivery worker are Phase 6 (IAM policies exist).
- Gateway A's settlement is polled with backoff until it arrives or the payment is parked; real
  settlement takes days, so that budget should be per state.
- Partial refunds and multi-attempt orders (gateway B) are still modelled as one payment = one state.

## Interview talking points

- At-least-once + idempotent receiver = exactly-once effect; where each hop's dedup lives.
- Transactional outbox + CDC vs dual writes.
- Why one conditional write makes the webhook/poll race a non-issue.
- Corrections: why two observations, why conditional on version, why delivered as a separate kind.
- Verify-once-after-final vs polling forever; time-window queries.
- Backoff + jitter, per-gateway rate limit and circuit breaker; parking as a time budget.
- Capture vs release for stuck authorizations: saving the sale vs returning money in minutes.
