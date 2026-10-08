package dev.concord.simulator.dialect;

import java.util.SplittableRandom;

final class Ids {

    private static final String BASE62 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    private Ids() {
    }

    static String base62(SplittableRandom rng, int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(BASE62.charAt(rng.nextInt(BASE62.length())));
        }
        return sb.toString();
    }

    static String digits(SplittableRandom rng, int length) {
        StringBuilder sb = new StringBuilder(length);
        sb.append(1 + rng.nextInt(9));
        for (int i = 1; i < length; i++) {
            sb.append(rng.nextInt(10));
        }
        return sb.toString();
    }
}
