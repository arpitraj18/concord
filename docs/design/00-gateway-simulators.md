# Phase 0 — Gateway simulators and fault injection

## Problem

Concord's whole claim is "correct under duplicate, delayed, reordered and missing webhooks". Real gateway
sandboxes do not fail on demand, so correctness cannot be proven against them. We need a hostile world
that fails exactly how we ask, and fails the same way every time we ask.

## What was built

One Spring Boot app, `gateway-simulator`, that speaks one of three gateway dialects (`SIM_GATEWAY=A|B|C`).
Docker Compose runs all three as separate processes so each can be slow or down on its own.

| | Gateway A | Gateway B | Gateway C |
|---|---|---|---|
| Loosely modelled on | Razorpay | Cashfree | Stripe PaymentIntents |
| Flow | authorize, then capture | auto-capture; several attempts per order | intent may need capture |
| Amount | integer paise (`49900`) | decimal rupees (`499.00`) | integer minor units, lowercase currency |
| Event id | `X-GwA-Event-Id` header | **none** | `id` field in body |
| Signature | `X-GwA-Signature: hex(HMAC(secret, body))` | `x-gwb-signature: base64(HMAC(secret, ts + body))` + `x-gwb-timestamp` | `GwC-Signature: t=<unix>,v1=hex(HMAC(secret, t + "." + body))` |
| Refund | `refund.processed` | `REFUND_STATUS_WEBHOOK`, different payload shape | `charge.refunded` on a *charge*, intent stays `succeeded` |

The differences are deliberate. Normalising them is Concord's job, and the AI onboarding assistant in
Phase 5 will have to map them.

## Key design decisions

### 1. Plan first, then execute

`DeliveryPlanner` is a pure function: `(scenario, seed) -> DeliveryPlan`. It decides every webhook,
every fault and every delivery time up front, with no clock, no I/O and no shared state.
`WebhookDispatcher` then only executes the plan against the clock.

*Why:* determinism is trivially testable (`plan(s) == plan(s)`), a plan can be inspected before running
(`POST /sim/plan`), and the plan doubles as ground truth for checking Concord later.

*Trade-off:* gateway **retries** (resending on non-2xx) are not in the plan, because they depend on how
the receiver responds. That is correct: retries are a reaction, not a fault we inject.

### 2. One random stream per payment, not one global stream

Every payment draws from `Seeds.rng(seed, purpose, index)`. With a single shared `Random`, adding one
directive to payment 0 would shift every random decision for payments 1..n, so editing a scenario would
silently change everything. A property test checks this isolation.

### 3. The world is always legal; only the messages lie

`SimState` has a real-world transition table (`CAPTURED -> AUTHORIZED` is impossible). Scenarios that
describe an impossible world are rejected with HTTP 400. Faults only affect *delivery*: drop, delay,
duplicate (same or new event id), or a contradiction (a webhook claiming a state that never happened).
The status API always serves the true timeline, so Concord has a source of truth to fall back on —
just like real gateways, where the status API is documented as more reliable than webhooks.

The simulator's states are **not** Concord's canonical states. If Concord's state machine and the
simulator shared code, one bug could hide in both.

### 4. Virtual time with a time scale

Scenarios are written in virtual milliseconds. `timeScale: 20` runs a 60 s scenario in 3 s. Retry
backoff and status-API outage windows are also virtual, so everything speeds up together.

### 5. Scripted directives override random faults

Random faults (`faults`) give coverage; directives give exact, named stories. If any directive targets a
webhook, random faults skip that webhook, so a scripted scenario stays exact even with background chaos on.

## Scenario format

```jsonc
{
  "name": "captured-first-duplicate-authorized-drop-settled",
  "seed": 42,                       // omit and one is chosen and returned, so any run can be replayed
  "targetUrl": "http://...",        // default: this simulator's /dev/sink
  "timeScale": 1,
  "payments": [{                    // hand-written payments; ids are generated from the seed if omitted
    "paymentId": "pay_demo0000000001", "amount": 49900, "currency": "INR",
    "steps": [ {"state": "AUTHORIZED", "atMs": 0}, {"state": "CAPTURED", "atMs": 1000}, {"state": "SETTLED", "atMs": 6000} ]
  }],
  "generate": { "count": 1000, "spreadMs": 60000, "stepGapMs": 5000 },   // random legal lifecycles
  "faults": { "dropRate": 0.05, "duplicateRate": 0.3, "maxExtraCopies": 3, "newEventIdRate": 0.1,
              "maxDelayMs": 8000, "maxDuplicateDelayMs": 15000, "contradictionRate": 0.02 },
  "directives": [                   // exact faults: DROP, DELAY, DUPLICATE, DUPLICATE_NEW_ID, CONTRADICT
    { "paymentId": "pay_demo0000000001", "state": "AUTHORIZED", "action": "DELAY", "delayMs": 4000 }
  ],
  "statusApi": { "errorRate": 0.05, "latencyMs": 20, "outages": [ {"fromMs": 5000, "toMs": 35000} ] }
}
```

Ready-made scenarios live in `gateway-simulator/scenarios/`.

## API

| Method | Path | Purpose |
|---|---|---|
| GET | `/v1/payments/{id}` | **Status API** in the gateway's own format (needs `Authorization: Bearer <SIM_API_KEY>` if set) |
| GET | `/sim/info` | Dialect, supported states, event names, lifecycle catalogue |
| POST | `/sim/plan` | Dry run: return the plan without sending anything |
| POST | `/sim/runs?start=true` | Create (and by default start) a run |
| GET | `/sim/runs`, `/sim/runs/{id}` | Run status, plan summary, every delivery attempt |
| GET | `/sim/runs/{id}/plan` | Full plan |
| GET | `/sim/runs/{id}/truth` | Current and final true state of every payment |
| POST | `/sim/runs/{id}/start`, `/cancel` | Control |
| POST/GET/DELETE | `/dev/sink` | Throwaway webhook receiver for demos before Concord exists |

Configuration (`application.yml` / env): `SIM_GATEWAY`, `SIM_WEBHOOK_SECRET`, `SIM_API_KEY`,
`SIM_TARGET_URL`, `SIM_TIME_SCALE`, `PORT`, and `simulator.retry.*` (default: 4 attempts, 2 s backoff, x2).

## Tests

- `DeliveryPlannerTest` — the Phase 0 "done when" scenario produces exactly
  `captured@1000, authorized@4000, authorized(duplicate, same id)@4500, settled DROPPED`; replay equality;
  validation errors.
- `DeliveryPlannerPropertiesTest` (jqwik, ~1,000 random scenarios × 3 gateways) — the plan is a pure
  function; every real step has exactly one original webhook; nothing is delivered before it happens;
  only contradictions claim false states; generated lifecycles are legal.
- `SignatureTest` — signatures match vectors produced independently with `openssl`.
- `SimulatorEndToEndTest`, `OtherGatewaysEndToEndTest` — real HTTP: replay over the wire, retries on 500,
  status API serves truth after a dropped webhook, outages return 503, signatures verify for all three.

## Done when — met

> A scripted scenario such as "send captured, then duplicate authorized 3 s later, then drop the settled
> webhook" replays identically.

`scenarios/phase0-done-when.json`, checked at plan level and over real HTTP (two runs, identical event
types and event ids, in identical order).

## Interview talking points

- Why simulators first: you cannot prove correctness against a sandbox that won't fail on demand.
- Determinism by separating a pure planner from a side-effecting dispatcher.
- Why per-payment seeded streams (edit stability).
- At-least-once delivery is the gateway's contract: duplicates with the same id are retries, duplicates
  with a new id defeat naive dedup — Concord must also dedup on meaning (payment + state).
- Gateway B has no event id at all: Phase 1 must derive a dedup key from content.

## Notes for Phase 1

- Write ingestion's signature verification **independently** (from the table above), and test it against
  real simulator output. Sharing the signing code would let a bug pass on both sides.
- Gateway C's signature carries a timestamp; reject stale ones (replay protection).
- Use `/sim/runs/{id}/truth` as the oracle when checking Concord's final states.
