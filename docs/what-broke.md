# What broke

Bugs and surprises found by fault injection. Each one is a story for interviews: what happened, why,
and what changed.

## 2026-10-08 — Webhooks arrived out of order with zero faults injected

**Phase:** 0 (simulator end-to-end test, gateway B and C)

**What happened:** A test sent `PENDING` at 0 ms and `SUCCESS` at 100 ms (virtual) with no faults and
expected them in that order. At `timeScale: 20` they were 5 ms apart in real time, and the success
webhook sometimes arrived first.

**Why:** each delivery is sent on its own virtual thread, so two requests a few milliseconds apart race
over the network. Real gateways behave the same way: they deliver concurrently and make no ordering
promise.

**Lesson / change:** ordering cannot be assumed even without failures; it must come from the data (state
rank), not from arrival order. The test now looks webhooks up by type. This is the exact reason Phase 2
applies an event only if it is a legal forward transition.

## 2026-10-08 — Every webhook rejected: the test was talking to the wrong gateway

**Phase:** 1 (simulator → ingestion test)

**What happened:** All ~50,000 deliveries in the first end-to-end run were rejected: "signature
mismatch" for A, "missing x-gwb-signature" for B, "missing gwc-signature" for C.

**Why:** the test started the simulator with `SpringApplicationBuilder.properties(...)`. Those are
*default* properties, the lowest precedence in Spring Boot, so the simulator's own `application.yml`
(`gateway: ${SIM_GATEWAY:A}`) won. All three runs were secretly gateway A with the dev secret.

**Lesson / change:** pass overrides as command-line arguments (highest precedence). On the bright side,
ingestion correctly refused every one of them — a free negative test.

## 2026-10-08 — "Archive count = attempts" was the wrong invariant

**Phase:** 1

**What happened:** the end-to-end test failed now and then: the archive held 21 fewer deliveries than
the simulator had attempted, yet every event was still enqueued exactly once.

**Why:** under a burst of thousands of near-simultaneous webhooks, the local server's accept queue (JDK
default backlog: 50) overflowed. Those requests got connect timeouts and never reached ingestion, so
there was nothing to archive. The simulator, like a real gateway, retried them and they succeeded. One
run also showed a genuine `503 IN_FLIGHT`: two copies of the same event raced and the claim protocol
made the loser retry.

**Lesson / change:** the true invariant is "every delivery that *reached* us is archived"
(`responded ≤ archived ≤ attempts`), plus "every delivery was eventually acknowledged". Raised the
local server's backlog to 1024. In AWS, API Gateway throttling plays the same role, which is why the
Terraform sets rate and burst limits and why answering non-2xx is safe: the gateway will retry.

## 2026-10-09 — A dropped webhook made Concord claim the payment was "unknown"

**Phase:** 2 (state machine unit test)

**What happened:** with `authorized` dropped and `captured` arriving first, the engine recorded the
inferred path `CREATED → PENDING → UNKNOWN → CAPTURED`.

**Why:** the inferred path was a breadth-first search over the transition graph. Both routes to
CAPTURED have the same length, and UNKNOWN is declared before AUTHORIZED, so it was found first. The
state was still legal, but the history said "the payment was in an unknown state", which is false and
would have triggered the resolver for no reason.

**Lesson / change:** "legal" is not the same as "true". Path-finding now tries the normal route first
and uses UNKNOWN only when nothing else reaches the target.

## 2026-10-09 — Docker builds would have failed: `-Dmaven.test.skip` skips the test-jar too

**Phase:** 2 (packaging)

**What happened:** the state engine reuses ingestion's test helpers through a test-jar. The Dockerfiles
built with `-Dmaven.test.skip=true`, which also skips *producing* that test-jar, and Maven still
resolves test-scoped dependencies before running a skipped plugin. A clean build failed with
"Could not find artifact dev.concord:ingestion:jar:tests".

**Lesson / change:** caught by simulating the Docker build from an empty local repository. Use
`-DskipTests` (compile tests, don't run them). Build tooling deserves the same "try it from clean"
discipline as code.

## 2026-10-10 — Payments parked before they could be marked UNKNOWN or captured

**Phase:** 3 (resolver tests)

**What happened:** with "park after 6 attempts" and a 10 s base backoff, an unreachable PENDING payment
was handed to a human before the 10-minute UNKNOWN deadline, and a long-AUTHORIZED payment was parked
before the 30-minute capture/release decision.

**Why:** with exponential backoff, an attempt count is really a give-up *time* nobody chose. "Still
authorized" confirmations also counted as attempts.

**Change:** parking is a time budget (`parkAfter`), and the config refuses one shorter than the
UNKNOWN or capture thresholds. Attempts only drive the backoff.

## 2026-10-10 — Backoff jitter delayed telling the merchant "uncertain"

**Phase:** 3 (flaky test, 2 failures in 5 runs)

**What happened:** a test expecting UNKNOWN after 13 minutes sometimes still saw PENDING.

**Why:** retries are jittered up to 5 minutes, so the first check after the 10-minute deadline could
come much later. Not just a test problem: the merchant should hear "do not cancel" on time.

**Change:** a PENDING payment's next check is never scheduled past its UNKNOWN deadline. Lesson: run a
new test several times before trusting it; jitter makes failures intermittent.

## 2026-10-10 — Time-window queries were wrong within the same second (latent since Phase 2)

**Phase:** 3 (found while adding `BETWEEN` queries)

**What happened:** GSI sort keys were `Instant.toString()`, which drops trailing zeros:
`10:00:00Z` sorts *after* `10:00:00.500Z` as text. "Stuck since T" queries could miss or include the
wrong payments around every second boundary.

**Change:** index sort keys use a fixed-width format (9 fraction digits). A contract test with two
payments 500 ms apart across a whole second now runs on DynamoDB Local. Lesson: an index sorts bytes,
not meaning.

## 2026-10-10 — The system test passed without ever testing the merchant's dedup

**Phase:** 3

**What happened:** the first green run reported `duplicatesAbsorbed=0`: no duplicate ever reached the
merchant, so "zero duplicate effects" proved nothing about its deduplication.

**Change:** after convergence, the test simulates a delivery-worker crash and restart (the stream is
re-read from the start). The merchant now receives every event twice and its effects do not change.
Lesson: check that a test's hostile condition actually happened.

## 2026-10-10 — Two Spring Boot apps on one classpath took each other's config

**Phase:** 3 (system tests)

**What happened:** with the simulator and the merchant in one JVM, the merchant failed to start
("could not resolve merchant.signature-tolerance-seconds"), then the simulator failed ("no suitable
JDBC driver").

**Why:** both shipped `application.yml`; Spring loads the first one on the classpath, so the merchant
silently got the simulator's settings. And the merchant's JDBC starter made the simulator try to
configure a database.

**Change:** the merchant uses its own config name (`sample-merchant.yml`); the simulator explicitly
excludes database auto-configuration.
