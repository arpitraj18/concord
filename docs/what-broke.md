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
