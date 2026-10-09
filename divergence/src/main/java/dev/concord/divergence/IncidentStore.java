package dev.concord.divergence;

import java.util.List;
import java.util.Optional;

public interface IncidentStore {

    Optional<Incident> find(String paymentKey, DivergenceType type);

    void put(Incident incident);

    List<Incident> forPayment(String paymentKey);

    /** Open incidents, most severe first. */
    List<Incident> open(int limit);
}
