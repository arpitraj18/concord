# Concord

**Every system agrees on what happened to the money.**

Concord is a gateway-agnostic payment state consistency engine. Payment gateways notify merchants through
webhooks that are duplicated, delayed, out of order or never sent at all. Concord turns those signals into
one verified, exactly-once stream of final payment state, resolves payments stuck in unknown states by
asking the gateway's status API, and flags merchant orders that disagree with payment truth
(paid-but-cancelled, unpaid-but-fulfilled).

It does not route payments and it never moves money. It is the consistency layer that sits beside a
gateway or an orchestrator.

## Status

| Phase | What | State |
|---|---|---|
| 0 | Gateway simulators + seeded fault injection | **done** |
| 1 | Ingestion + raw event log (Lambda, S3, SQS FIFO, dedup) | **done** |
| 2 | Canonical state machine + DynamoDB state store | **done** |
| 3 | Uncertainty resolver + exactly-once delivery (outbox) | **done** |
| 4 | Divergence detector + dashboard | next |
| 5 | AI layer: gateway onboarding, anomaly detection, incident explanations | planned |
| 6 | Hardening, load/chaos testing, deployment | planned |
| 7 | (stretch) on-chain stablecoin settlement source | planned |

## Architecture (target)

```
Gateway simulators A/B/C ──webhooks (dup / late / reordered / dropped)──┐
                                                                       v
             API Gateway + Ingest Lambda (verify HMAC, archive to S3, dedup, ack)
                                                                       v
                                  SQS FIFO (message group = payment_id)
                                                                       v
     State engine (canonical state machine, DynamoDB conditional writes, versioning)
        │                     │ unknown / stuck                       │ final state (+ outbox, one txn)
        │                     v                                       v
        │      Resolver (backoff polling of status API)     DynamoDB Streams -> delivery worker
        │                                                              v
        v                                               Merchant backend (dedupes by key)
  Divergence detector (gateway vs Concord vs merchant order DB)
        v
  AI layer (Python/FastAPI + Bedrock)
```

## Repository layout

```
gateway-simulator/   Phase 0: three fake gateways with deterministic fault injection (Java 21, Spring Boot)
  scenarios/         Reusable scenario files
ingestion/           Phase 1: verify, archive, dedupe, enqueue (plain Java 21; AWS Lambda or local server)
state-engine/        Phase 2: canonical state machine, gateway mappings (JSON), DynamoDB store, SQS worker, replay
resolver/            Phase 3: status-API resolver (backoff, circuit breaker, corrections, capture/release)
delivery/            Phase 3: outbox via DynamoDB Streams -> merchant, in order, retried, dead-lettered
sample-merchant/     Phase 3: a shop on PostgreSQL that applies Concord's events exactly once (no shared code)
local-runner/        All of Concord in one process, in memory (local development, docker compose)
system-tests/        End-to-end tests of the whole system against the hostile simulator
infra/terraform/     AWS infrastructure (S3, DynamoDB, SQS FIFO, Lambda, API Gateway), LocalStack-capable
docs/design/         One design note per phase: problem, options, choice, why
docs/what-broke.md   Bugs found by fault injection (interview stories)
scripts/             Helper scripts
```

## Quick start

Requirements: Java 21, Maven 3.9 (Docker optional).

```bash
mvn verify                                   # build + all tests (unit, property-based, end-to-end)

# run one gateway locally
SIM_GATEWAY=A java -jar gateway-simulator/target/gateway-simulator-*-exec.jar      # port 8081

# or the whole system: gateways A/B/C (8081-8083) -> Concord (8080) -> merchant (8090) on PostgreSQL
docker compose up --build
```

### Try it (with docker compose running)

```bash
# 1. plan a hostile scenario and let the merchant create an order for every payment
#    (the merchant registers each payment with Concord as it creates the order)
curl -s -X POST localhost:8081/sim/plan -H 'Content-Type: application/json' \
     -d '{"seed":7,"timeScale":10,"generate":{"count":50,"spreadMs":20000,"stepGapMs":3000},"faults":{"dropRate":0.5}}' \
  | python3 -c 'import json,sys,urllib.request as u
for p in json.load(sys.stdin)["payments"]:
    u.urlopen(u.Request("http://localhost:8090/orders", json.dumps({"orderId":p["orderId"],"gateway":"A",
      "paymentId":p["paymentId"],"amountMinor":p["amount"],"currency":"INR"}).encode(), {"Content-Type":"application/json"}))'
# 2. run it: half the webhooks are dropped
curl -s -X POST localhost:8081/sim/runs -H 'Content-Type: application/json' \
     -d '{"seed":7,"timeScale":10,"generate":{"count":50,"spreadMs":20000,"stepGapMs":3000},"faults":{"dropRate":0.5}}'
# 3. watch Concord recover from the status API, and the merchant apply each event once
curl -s localhost:8080/local/resolver; curl -s localhost:8080/local/delivery; curl -s localhost:8090/stats
```

Drive it:

```bash
curl -s localhost:8081/sim/info                                         # what gateway A speaks
scripts/run-scenario.sh gateway-simulator/scenarios/phase0-done-when.json
curl -s localhost:8081/dev/sink                                         # webhooks that arrived (simulator alone)
curl -s localhost:8080/local/stats                                      # archived / enqueued (with docker compose)
curl -s localhost:8080/local/payments/a/pay_demo0000000001              # canonical state + history
curl -s localhost:8081/v1/payments/pay_demo0000000001 \
     -H 'Authorization: Bearer key_dev_gateway_a'                       # status API (truth)
```

Design notes, with the reasoning behind every decision:
[Phase 0 — simulators](docs/design/00-gateway-simulators.md),
[Phase 1 — ingestion](docs/design/01-ingestion.md),
[Phase 2 — state machine](docs/design/02-state-machine.md),
[Phase 3 — resolver and delivery](docs/design/03-resolver-and-delivery.md).

Deploy ingestion to AWS (or LocalStack with `-var localstack=true`):

```bash
mvn -pl ingestion -am package -DskipTests
cd infra/terraform && cp terraform.tfvars.example terraform.tfvars   # set secrets + budget email
terraform init && terraform apply
terraform destroy                                                    # after every session
```

## Measured results

Numbers are added here only once they have actually been measured.
