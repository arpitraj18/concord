package dev.concord.resolver.status;

import dev.concord.ingest.Gateway;

/** The source of truth: each gateway's status API. Read-only keys only; Concord never moves money here. */
public interface StatusClient {

    StatusResult fetch(Gateway gateway, String paymentId);
}
