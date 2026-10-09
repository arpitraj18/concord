package dev.concord.engine.store;

class InMemoryPaymentStoreTest extends PaymentStoreContract {

    private final InMemoryPaymentStore store = new InMemoryPaymentStore();

    @Override
    protected PaymentStore store() {
        return store;
    }
}
