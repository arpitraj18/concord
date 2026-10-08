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
| 1 | Ingestion + raw event log (Lambda, S3, SQS FIFO, dedup) | next |
| 2 | Canonical state machine + DynamoDB state store | planned |
| 3 | Uncertainty resolver + exactly-once delivery (outbox) | planned |
| 4 | Divergence detector + dashboard | planned |
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
docs/design/         One design note per phase: problem, options, choice, why
docs/what-broke.md   Bugs found by fault injection (interview stories)
scripts/             Helper scripts
```

## Quick start

Requirements: Java 21, Maven 3.9 (Docker optional).

```bash
mvn verify                                   # build + all tests (unit, property-based, end-to-end)

# run one gateway locally
SIM_GATEWAY=A java -jar gateway-simulator/target/gateway-simulator-*.jar      # port 8081

# or all three gateways (A: 8081, B: 8082, C: 8083)
docker compose up --build
```

Drive it:

```bash
curl -s localhost:8081/sim/info                                         # what gateway A speaks
scripts/run-scenario.sh gateway-simulator/scenarios/phase0-done-when.json
curl -s localhost:8081/dev/sink                                         # webhooks that arrived
curl -s localhost:8081/v1/payments/pay_demo0000000001 \
     -H 'Authorization: Bearer key_dev_gateway_a'                       # status API (truth)
```

See [docs/design/00-gateway-simulators.md](docs/design/00-gateway-simulators.md) for the full API and the
reasoning behind it.

## Measured results

Numbers are added here only once they have actually been measured.
