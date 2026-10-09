package dev.concord.divergence;

import dev.concord.resolver.merchant.MerchantOrders;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public interface DetectorStateStore {

    Optional<DetectorState> get(String paymentKey);

    void put(DetectorState state);

    /** Where the merchant change feed was last read up to. */
    MerchantOrders.Cursor merchantCursor();

    void saveMerchantCursor(MerchantOrders.Cursor cursor);

    final class InMemory implements DetectorStateStore {
        private final Map<String, DetectorState> states = new ConcurrentHashMap<>();
        private volatile MerchantOrders.Cursor cursor = MerchantOrders.Cursor.start();

        @Override
        public Optional<DetectorState> get(String paymentKey) {
            return Optional.ofNullable(states.get(paymentKey));
        }

        @Override
        public void put(DetectorState state) {
            states.put(state.paymentKey(), state);
        }

        @Override
        public MerchantOrders.Cursor merchantCursor() {
            return cursor;
        }

        @Override
        public void saveMerchantCursor(MerchantOrders.Cursor c) {
            cursor = c;
        }
    }
}
