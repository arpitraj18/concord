package dev.concord.ingest;

/** What happened to one delivery, and which HTTP status tells the gateway the right thing. */
public enum IngestResult {
    /** New event, archived and enqueued. */
    ACCEPTED(200),
    /** Already enqueued earlier; archived only. 200 so the gateway stops retrying. */
    DUPLICATE(200),
    /** Authentic but not understood; archived for a human. 200 because retrying cannot fix it. */
    UNPARSEABLE(200),
    /** Same event is being processed right now; 503 asks the gateway to retry later. */
    IN_FLIGHT(503),
    /** Signature failed; archived under rejected/. */
    REJECTED(401),
    UNKNOWN_GATEWAY(404),
    /** Archive or queue unavailable; nothing acknowledged, the gateway's retry will try again. */
    ERROR(500);

    private final int httpStatus;

    IngestResult(int httpStatus) {
        this.httpStatus = httpStatus;
    }

    public int httpStatus() {
        return httpStatus;
    }
}
