package dev.concord.resolver.state;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public final class InMemoryResolverStateStore implements ResolverStateStore {

    private final Map<String, ResolverState> states = new ConcurrentHashMap<>();

    @Override
    public Optional<ResolverState> get(String paymentKey) {
        return Optional.ofNullable(states.get(paymentKey));
    }

    @Override
    public void put(ResolverState state) {
        states.put(state.paymentKey(), state);
    }

    @Override
    public List<ResolverState> parked() {
        return states.values().stream().filter(ResolverState::parked).toList();
    }
}
