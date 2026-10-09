package dev.concord.resolver.state;

import java.util.List;
import java.util.Optional;

public interface ResolverStateStore {

    Optional<ResolverState> get(String paymentKey);

    void put(ResolverState state);

    /** Payments the resolver gave up on: the human review queue. */
    List<ResolverState> parked();
}
