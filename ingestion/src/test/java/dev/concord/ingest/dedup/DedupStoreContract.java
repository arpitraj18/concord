package dev.concord.ingest.dedup;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;

/**
 * The rules every DedupStore must follow. Run against the in-memory store and against DynamoDB Local,
 * so the fake used in fast tests is proven to behave like the real thing.
 */
public abstract class DedupStoreContract {

    protected static final Duration LEASE = Duration.ofSeconds(30);
    protected static final Duration TTL = Duration.ofDays(7);
    protected static final Instant T0 = Instant.parse("2026-10-08T12:00:00Z");

    protected abstract DedupStore store();

    protected String key(String name) {
        return getClass().getSimpleName() + "#" + name + "#" + System.nanoTime();
    }

    @Test
    void firstClaimWinsThenDuplicatesAreRecognised() {
        String k = key("dup");
        assertThat(store().claim(k, "t1", T0)).isEqualTo(ClaimOutcome.CLAIMED);
        store().markEnqueued(k, "t1", T0);
        assertThat(store().claim(k, "t2", T0.plusSeconds(1))).isEqualTo(ClaimOutcome.DUPLICATE);
        assertThat(store().claim(k, "t3", T0.plus(Duration.ofDays(1)))).isEqualTo(ClaimOutcome.DUPLICATE);
    }

    @Test
    void unfinishedClaimIsInFlightUntilLeaseExpiresThenCanBeTakenOver() {
        String k = key("lease");
        assertThat(store().claim(k, "crashed", T0)).isEqualTo(ClaimOutcome.CLAIMED);
        assertThat(store().claim(k, "t2", T0.plusSeconds(5))).isEqualTo(ClaimOutcome.IN_FLIGHT);
        assertThat(store().claim(k, "t3", T0.plus(LEASE).plusSeconds(1))).isEqualTo(ClaimOutcome.TAKEN_OVER);
    }

    @Test
    void staleOwnerCannotMarkAfterTakeover() {
        String k = key("stale");
        store().claim(k, "old", T0);
        store().claim(k, "new", T0.plus(LEASE).plusSeconds(1));
        store().markEnqueued(k, "old", T0.plus(LEASE).plusSeconds(2));
        assertThat(store().claim(k, "probe", T0.plus(LEASE).plusSeconds(3)))
                .as("old token must not have marked it enqueued").isEqualTo(ClaimOutcome.IN_FLIGHT);
        store().markEnqueued(k, "new", T0.plus(LEASE).plusSeconds(4));
        assertThat(store().claim(k, "probe2", T0.plus(LEASE).plusSeconds(5))).isEqualTo(ClaimOutcome.DUPLICATE);
    }

    @Test
    void releaseLetsTheNextDeliveryClaimImmediately() {
        String k = key("release");
        store().claim(k, "t1", T0);
        store().release(k, "t1");
        assertThat(store().claim(k, "t2", T0.plusSeconds(1))).isEqualTo(ClaimOutcome.CLAIMED);
    }

    @Test
    void releaseWithWrongTokenOrAfterEnqueueDoesNothing() {
        String k = key("release-guard");
        store().claim(k, "t1", T0);
        store().release(k, "intruder");
        assertThat(store().claim(k, "t2", T0.plusSeconds(1))).isEqualTo(ClaimOutcome.IN_FLIGHT);
        store().markEnqueued(k, "t1", T0);
        store().release(k, "t1");
        assertThat(store().claim(k, "t3", T0.plusSeconds(2))).isEqualTo(ClaimOutcome.DUPLICATE);
    }

    @Test
    void expiredRecordCountsAsNew() {
        String k = key("ttl");
        store().claim(k, "t1", T0);
        store().markEnqueued(k, "t1", T0);
        assertThat(store().claim(k, "t2", T0.plus(TTL).plusSeconds(1))).isEqualTo(ClaimOutcome.CLAIMED);
    }

    @Test
    void concurrentClaimsHaveExactlyOneWinner() throws Exception {
        String k = key("race");
        int threads = 32;
        CountDownLatch go = new CountDownLatch(1);
        List<Callable<ClaimOutcome>> tasks = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            String token = "t" + i;
            tasks.add(() -> {
                go.await();
                return store().claim(k, token, T0);
            });
        }
        try (ExecutorService pool = Executors.newFixedThreadPool(threads)) {
            List<Future<ClaimOutcome>> futures = new ArrayList<>();
            tasks.forEach(t -> futures.add(pool.submit(t)));
            go.countDown();
            List<ClaimOutcome> outcomes = new ArrayList<>();
            for (Future<ClaimOutcome> f : futures) {
                outcomes.add(f.get());
            }
            assertThat(outcomes).filteredOn(o -> o == ClaimOutcome.CLAIMED).hasSize(1);
            assertThat(outcomes).filteredOn(o -> o == ClaimOutcome.IN_FLIGHT).hasSize(threads - 1);
        }
    }
}
