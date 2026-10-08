# Phase 1 — Ingestion and the raw event log

## Problem

Accept every webhook safely and never lose one, even though the same event arrives several times, out
of order, from three gateways that sign and shape their payloads differently, and even if ingestion
itself crashes halfway through.

```
Gateway ──> API Gateway ──> Ingest Lambda: verify ─> archive (S3) ─> claim (DynamoDB) ─> enqueue (SQS FIFO) ─> mark ─> 200
```

## What was built

Module `ingestion/` (plain Java 21, no Spring, for Lambda cold starts):

| Piece | Class | Notes |
|---|---|---|
| Signature check | `GatewayAVerifier`, `GatewayBVerifier`, `GatewayCVerifier` | Written from each scheme, **not** sharing code with the simulator; constant-time compare; timestamp tolerance (5 min) for B and C; several secrets for rotation |
| What it's about | `EnvelopeExtractor` | Payment id (ordering) and dedup key, nothing more |
| Raw archive | `S3RawArchive` | `raw/gateway=A/dt=YYYY-MM-DD/hour=HH/<ms>-<uuid>.json`, exact body bytes in base64 |
| Dedup | `DynamoDbDedupStore` | Claim / mark / release protocol (below) |
| Queue | `SqsEventQueue` | FIFO, `MessageGroupId = gateway#paymentId`, `MessageDeduplicationId = sha256(dedupKey)` |
| Entry points | `IngestHandler` (Lambda, API Gateway HTTP API v2), `LocalIngestServer` (local) | Same `IngestionService` behind both |
| Infra | `infra/terraform/` | S3, DynamoDB (TTL), SQS FIFO + DLQ, Lambda (SnapStart, arm64), HTTP API with throttling, least-privilege IAM, budget alert, LocalStack switch |

## Design decisions

### 1. Acknowledge fast, but only after the event is safe

Ingestion does the minimum (verify, archive, dedupe, enqueue) and returns 200 within milliseconds;
all state logic happens later, off the queue. Slow handlers cause gateway timeouts, which cause
retries, which cause more duplicates.

But "fast" never means "before it is durable". The 200 is sent only after the raw delivery is in S3
and the event is either on the queue or known to be there already. Every failure returns non-2xx so
the gateway's own retry becomes our recovery mechanism:

| Situation | Response | Why |
|---|---|---|
| New event | 200 `ACCEPTED` | archived + enqueued |
| Already enqueued | 200 `DUPLICATE` | archived only |
| Valid signature, payload not understood | 200 `UNPARSEABLE` | archived for a human; retrying cannot fix it |
| Same event being processed right now | 503 `IN_FLIGHT` | gateway retries in a moment |
| Bad signature | 401 `REJECTED` | archived under `rejected/` (expires in 30 days) |
| S3 / DynamoDB / SQS failure | 500 `ERROR` | nothing acknowledged; gateway retries |

### 2. Dedup is claim → enqueue → mark, not "insert if absent"

The obvious design (conditional put on `event_id`, then enqueue) has a hole: if the Lambda dies after
the put and before the enqueue, every retry from the gateway is treated as a duplicate and the event
is **lost forever**. So:

1. **Claim**: one conditional `PutItem` writes `{status: CLAIMED, claim_token, claimed_at}`. The
   condition allows it when the key is new, when the old item is past its TTL, or when the old item is
   a claim older than the lease (30 s). Because it is a single conditional write, two racing deliveries
   cannot both win (a 32-thread race test proves exactly one winner).
2. **Enqueue** to SQS FIFO.
3. **Mark** `status: ENQUEUED`, conditional on our `claim_token`, so a slow instance that lost its
   lease cannot overwrite the instance that took over.

Failure cases:

- Crash after claim, before enqueue → the claim goes stale → the next retry after 30 s **takes over**
  and enqueues. Until then retries get 503.
- Enqueue fails → claim is **released** at once, so the retry can proceed immediately.
- Enqueue succeeds but mark fails → a later retry may take over and send again; the queue's
  `MessageDeduplicationId` drops that re-send inside SQS's 5-minute window, and the state engine
  (Phase 2) is idempotent beyond it. Exactly-once *effect*, not exactly-once delivery.

The `ReturnValuesOnConditionCheckFailure=ALL_OLD` option tells us *why* a claim failed (already
enqueued vs. in flight) without a second read.

### 3. What is the identity of an event?

- **A and C** send an event id → key `A#evt_...`.
- **B sends no event id at all** → key is a hash of what the event means:
  `payment id + event type + reported status` (+ `refund_id` for refunds).

The key is deliberately **not** "payment + state" for everyone: a payment can receive two
`refund.processed` events for two partial refunds; collapsing them would lose a money movement. So a
duplicate re-sent with a *fresh* event id passes ingestion, and the state engine treats the repeated
state as a no-op. Ingestion dedups deliveries; the state engine dedups meaning.

### 4. Why keep every raw delivery forever

Event sourcing: state can be rebuilt by replaying the archive. That gives an audit trail, bug recovery
(fix the code, replay), and realistic test data. Duplicates are archived too: the archive is a record
of what was *received*, not of what was *meant*. The exact body bytes are stored so signatures can be
re-verified on replay.

### 5. SQS FIFO with message groups

Events for one payment must be processed in order; different payments should run in parallel.
`MessageGroupId = gateway#paymentId` gives exactly that (payment ids are only unique per gateway).
One global order would cap throughput; no order at all would race. High-throughput FIFO mode
(`perMessageGroupId`) is enabled in Terraform.

Note that the *queue* order is arrival order, not event order — the gateways already shuffled them.
Phase 2 still has to rank states, not trust order.

### 6. Fakes that are proven to match the real thing

The dedup rules live in one abstract test (`DedupStoreContract`) that runs against both the in-memory
store and **DynamoDB Local** (embedded, no Docker). SQS behaviour is tested against **ElasticMQ**
(SQS-compatible, in-process, supports FIFO dedup). S3 is tested with Testcontainers where Docker
exists (CI). LocalStack now needs an auth token, so it is optional, not required.

## Done when — met

> 1,000 duplicated and shuffled webhooks result in each unique event enqueued exactly once and all
> raw events stored in S3.

`SimulatorToIngestionTest` starts the real Phase 0 simulator for each gateway (A, B, C), sends
1,000 payments with 50% duplication (up to 3 extra copies, 10% with fresh ids), up to 5 s of reordering
delay, 3% drops and 2% contradictions, and checks:

- every planned delivery was eventually acknowledged;
- every delivery that reached ingestion is in the archive;
- no dedup key was enqueued twice, and the queue was never even *called* twice for one event;
- the set of enqueued events equals the set of distinct delivered events (by event id for A and C, by
  payment + state for B).

`AwsAdaptersPipelineTest` repeats this through DynamoDB Local and an ElasticMQ FIFO queue, then drains
the queue.

## Running it

```bash
mvn -pl ingestion -am package
GATEWAY_A_SECRETS=whsec_dev_gateway_a java -cp ingestion/target/ingestion-*-lambda.jar \
    dev.concord.ingest.local.LocalIngestServer                        # :8080, in-memory backend
SIM_GATEWAY=A SIM_WEBHOOK_SECRET=whsec_dev_gateway_a SIM_TARGET_URL=http://localhost:8080/webhooks/a \
    java -jar gateway-simulator/target/gateway-simulator-*-exec.jar   # :8081
scripts/run-scenario.sh gateway-simulator/scenarios/chaos-1000.json
curl localhost:8080/local/stats
```

Deploying: `cd infra/terraform && cp terraform.tfvars.example terraform.tfvars && terraform apply`
(or `-var localstack=true` against LocalStack). Destroy after each session: `terraform destroy`.

## Known gaps (deliberate, for later phases)

- Secrets are Lambda environment variables (encrypted at rest); move to Secrets Manager or SSM.
- Gateway A's scheme has no timestamp, so a captured webhook could be replayed. Harmless here (a
  replay is a duplicate) but worth saying in an interview.
- No metrics yet beyond logs: Phase 6 adds counters per `IngestResult`.
- The S3 adapter test needs Docker; it runs in CI, not in sandboxes without Docker.

## Interview talking points

- "Insert if absent" dedup loses events on crash; claim/lease/mark with fencing tokens fixes it.
- Exactly-once delivery is impossible over a network; exactly-once *effect* = at-least-once + dedup ids
  at every hop + an idempotent consumer.
- Acknowledge fast, but never before durable; let the gateway's retry be your recovery.
- Partial refunds are why "payment + state" is the wrong dedup key.
- Why `MessageGroupId` includes the gateway.
