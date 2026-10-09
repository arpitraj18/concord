package dev.concord.engine.mapping;

/** Where a claimed state came from. Only the status API is authoritative (it is the source of truth). */
public enum EventSource {
    /** A gateway webhook: authentic but unreliable (late, duplicated, missing, sometimes wrong). */
    WEBHOOK,
    /** The gateway's status API, polled by the resolver: the source of truth. */
    STATUS_API,
    /** Concord's own judgement that a payment has been pending too long (PENDING -> UNKNOWN). */
    TIMEOUT,
    /** The merchant told Concord the payment exists (at order creation). */
    REGISTRATION
}
