package dev.concord.simulator.plan;

import java.util.SplittableRandom;

/**
 * Derives independent random streams from one scenario seed.
 *
 * <p>Each payment gets its own stream keyed by (seed, purpose, index). A single shared stream would
 * make every decision depend on every earlier one: adding one directive or one payment would shift
 * all later faults and make scenarios impossible to edit without changing everything.
 */
public final class Seeds {

    private Seeds() {
    }

    public static SplittableRandom rng(long seed, String purpose, int index) {
        long h = mix64(seed);
        h = mix64(h ^ purpose.hashCode());
        h = mix64(h ^ index);
        return new SplittableRandom(h);
    }

    /** Uniform in [0, max], or 0 when max <= 0. */
    public static long jitter(SplittableRandom rng, long max) {
        return max <= 0 ? 0 : rng.nextLong(max + 1);
    }

    public static boolean chance(SplittableRandom rng, double p) {
        return p > 0 && rng.nextDouble() < p;
    }

    // Stafford variant 13 finaliser, as used by SplittableRandom itself.
    static long mix64(long z) {
        z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L;
        z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL;
        return z ^ (z >>> 31);
    }
}
