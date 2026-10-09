# Phase 2 — Canonical state machine and state store

## Problem

One payment model for every gateway that can **never enter an invalid state**, no matter how webhooks
arrive: duplicated, late, out of order, missing, contradictory, or processed by two workers at once.

```
SQS FIFO ──> StateEngine: normalise (mapping JSON) ─> read ─> decide (StateMachine) ─> conditional commit
                                                                                          │
                       DynamoDB: STATE (versioned) + EVT#<dedupKey> (history) + OUTBOX#<v>, one transaction
```

## The state machine

```
Normal path:  CREATED -> PENDING -> AUTHORIZED -> CAPTURED -> SETTLED
Exceptions:   PENDING -> UNKNOWN        UNKNOWN -> AUTHORIZED | CAPTURED | FAILED
              PENDING -> FAILED         AUTHORIZED -> FAILED
              CAPTURED -> REFUNDED      SETTLED -> REFUNDED
```

The graph has no cycles, so "X comes before Y" is well defined (Y is reachable from X), and since there
are no backward edges, a payment can never go back. For each claimed state the machine decides:

| Claim vs current | Decision | Example |
|---|---|---|
| equal | `SAME` – no-op | duplicate sent with a new event id |
| reachable ahead | `APPLY` – move, possibly several steps | `CAPTURED` arrives first; PENDING, AUTHORIZED recorded as *inferred* |
| behind | `STALE` – recorded, ignored | `authorized` arrives after `captured` |
| other branch | `CONTRADICTION` – not applied, payment flagged | `failed` after `captured` |

**Timestamps are never used to order events.** Clocks across gateways and servers disagree; the
lifecycle itself is the clock.

### Decision: apply jumps, not only single steps

The plan said "applied only if it is an allowed forward transition". Strictly single steps would leave
a payment stuck forever when one webhook in the middle is dropped (and in the simulator, 15% are).
So a claim anywhere *ahead on a legal path* is applied, and the states skipped are recorded as inferred
in the history (`path`). The path prefers the normal route: inferring UNKNOWN for a merely dropped
webhook would be wrong (this was a real bug, see what-broke.md).

### Decision: what contradictions mean, and why order matters only there

Drawn as an order, the states form a "Y": one path up to AUTHORIZED, then FAILED on one branch and
CAPTURED → SETTLED → REFUNDED on the other. Every pair of states is ordered **except FAILED versus
money-moved**. So:

- If a payment's claims all lie on one path, its state is the furthest claim, **whatever the arrival
  order**. Proven by a property test over random shuffles.
- A payment is flagged **if and only if** its claims include FAILED and a money-moved state. Also
  independent of order.
- Which of the two contradicting claims the payment *holds* until verification does depend on which
  arrived first. Making that order-independent would require moving a payment backwards (CAPTURED →
  FAILED), which the machine forbids. And it should: the merchant may already have shipped. So the
  contradiction is flagged and the **resolver (Phase 3) settles it from the status API**, the source of
  truth. The order never decides.

This is an honest limit worth stating in an interview: you cannot have monotonic legal transitions,
order-independence, *and* contradictory inputs all at once; something has to give, and here it is a
flag plus an authoritative check.

## Gateway mappings are data

`src/main/resources/mappings/gateway-{a,b,c}.v1.json` translate each gateway's events to canonical
states and normalise amounts (B's decimal rupees → paise), currency case, order ids and timestamps.
Rules can have conditions (B's refund webhook only counts when `refund_status` is `SUCCESS`). Mappings
are validated on load (no rule may claim CREATED or UNKNOWN, no two unconditional rules for one event,
…), and every history entry records which mapping version decided it. In Phase 5 the LLM onboarding
assistant proposes exactly these files.

## Concurrency: optimistic, version-based

Every STATE item has a `version`. A write says "set state=CAPTURED, version=6 **only if version is
still 5**". The loser of a race re-reads and decides again. This beats locking because conflicts on
one payment are rare: SQS FIFO hands out one message per payment at a time, so a conflict needs a
redelivery, a resolver write (Phase 3) or a replay racing the live path. Tested with 8 threads on one
payment against both the in-memory store and DynamoDB Local: no lost update, outbox chain intact.

## Idempotency: the history item is the receipt

History items are keyed by dedup key (`SK = EVT#<dedupKey>`), not by time as first planned, and are
written with "must not exist". Processing the same event twice (an SQS redelivery, or a re-send after
an ingestion takeover hours later) fails that condition, and the whole transaction is cancelled.
`TransactionCanceledException` says *which* condition failed, so "already processed" and "version
conflict" are told apart without another read. The history is sorted by `received_at` when read.

## DynamoDB single-table design

```
PK = PAY#<gateway>#<paymentId>   SK = STATE              state, version, amount, currency, order, flags
                                 SK = EVT#<dedupKey>     one per event, applied or not, with outcome and path
                                 SK = OUTBOX#<version>   change to deliver to the merchant (Phase 3)
GSI1: gsi1pk = STATE#<state>, gsi1sk = updated_at         "payments in PENDING not updated for 15 min"
```

- The gateway is in the key: payment ids are unique only per gateway (the plan's `PAY#<payment_id>`
  would collide).
- One payment = one partition: reading a payment with its history is one Query.
- **Transactional outbox**: STATE, EVT and OUTBOX are written in one `TransactWriteItems`. Writing state
  and then separately publishing an event risks the dual-write problem: lost or phantom events on a crash.
  DynamoDB Streams (enabled in Terraform) will carry OUTBOX items to the delivery worker.
- Why DynamoDB, not PostgreSQL: the access pattern is almost always get/update one payment by id, which
  is key-value; conditional writes and transactions give safe transitions; Streams give CDC for the
  outbox. PostgreSQL would also work (row locks, transactions); it is used for the merchant order DB
  (Phase 4), where joins and ad-hoc queries matter.
- Known scaling issue: GSI1's partition key has only 8 values, so at very high volume it becomes a hot
  partition. Fix in Phase 6: shard it (`STATE#PENDING#<0..N>`) and query all shards.

## Consuming the queue

`SqsStateWorker` long-polls, processes, and deletes a message only after its commit. On a failure it
holds back the rest of that batch **for the same payment** (processing a later event first would break
per-payment order) while other payments continue. After 5 failures SQS moves the message to the DLQ.
`WorkerMain` runs several of these in a container (ECS Fargate in Phase 6).

## Done when — met

> Property-based tests over thousands of random event sequences never produce an illegal state, and
> replaying the S3 archive rebuilds identical state.

1. `StateEnginePropertiesTest` (jqwik, 8,000 random sequences including UNKNOWN, unmapped events,
   repeats, redeliveries and contradictions):
   - every change is a legal chain of single steps and the state never goes back;
   - consistent evidence gives the same state in any order, and the flag never depends on order;
   - flagged if and only if the claims contradict;
   - every distinct event is recorded once; redeliveries change nothing; version = number of changes.
2. `SimulatorToEngineTest`, end to end on all three gateways (1,000 payments each, 15% drops, 40%
   duplicates, 20% of them with fresh ids, 6 s reordering, 5% contradictions), state on DynamoDB Local:
   - **every payment matches an oracle computed independently from the simulator's plan** (the furthest
     delivered claim, or flagged when the delivered claims contradict);
   - amounts are correct in minor units on all gateways (B arrives in decimal rupees);
   - every outbox chain is legal;
   - **replaying the raw archive into an empty store rebuilds identical state**: 0 mismatches on
     consistent payments, identical flags. In one run, 1 flagged payment on gateway B held the other
     contradicting claim after replay, exactly the case predicted above.

Sample output:

```
gateway A: 960 payments, 912 match oracle, 48 flagged; vs truth: 99 behind (webhook dropped),
           6 ahead (lie consistent with a legal path); replay of 3234 deliveries: 0 mismatches
```

### What the numbers say about Phases 3 and 4

About 8–10% of payments end **behind** the truth because their last webhooks were dropped, and about
1% end **ahead** of it because a lie happened to agree with a legal path (e.g. a fake "captured" for a
payment that was only authorized). No webhook-only system can catch either. Only the status API can:
that is the resolver (Phase 3) and the divergence detector (Phase 4).

## Running it

```bash
mvn -pl state-engine -am package -DskipTests
GATEWAY_A_SECRETS=whsec_a java -cp state-engine/target/state-engine-*-all.jar dev.concord.engine.local.LocalConcord
SIM_GATEWAY=A SIM_WEBHOOK_SECRET=whsec_a SIM_TARGET_URL=http://localhost:8080/webhooks/a \
    java -jar gateway-simulator/target/gateway-simulator-*-exec.jar
scripts/run-scenario.sh gateway-simulator/scenarios/phase0-done-when.json
curl localhost:8080/local/payments/a/pay_demo0000000001
```

```
payment pay_demo0000000001 CAPTURED v1
   payment.captured     APPLIED  -> CAPTURED [CREATED, PENDING, AUTHORIZED, CAPTURED]
   payment.authorized   STALE    -> CAPTURED
```

(The settled webhook was dropped, so Concord says CAPTURED while the status API says settled; Phase 3.)

## Known gaps

- REFUNDED means fully refunded; partial refund amounts are not tracked yet.
- Correcting a flagged payment from the status API needs an explicit, audited correction transition:
  designed in Phase 3 with the resolver.
- Gateway B models one payment attempt per id; aggregating attempts per order is for Phase 4.
- The outbox is written but not yet delivered (Phase 3).

## Interview talking points

- Why rank by lifecycle, not by timestamp.
- Apply-if-reachable vs single steps: dropped webhooks would strand payments.
- The "Y-shaped" order: where order-independence is possible and where it provably is not.
- Optimistic concurrency with versions; why it beats locks here.
- Idempotent consumer: the history item doubles as the processed-event receipt.
- Transactional outbox in DynamoDB; why not dual writes.
- Partition key includes the gateway; hot GSI partition and how to shard it.
