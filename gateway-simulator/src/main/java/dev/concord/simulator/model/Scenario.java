package dev.concord.simulator.model;

import java.util.List;

/**
 * A reproducible description of a run: which payments exist, how they really progress, and which
 * faults hit their webhooks. Same scenario + same seed = same delivery plan, every time.
 *
 * @param timeScale virtual ms per real ms; 10 makes a 60 s scenario finish in 6 s
 */
public record Scenario(
        String name,
        Long seed,
        String targetUrl,
        Double timeScale,
        FaultProfile faults,
        StatusApiFaults statusApi,
        List<PaymentSpec> payments,
        GeneratedPayments generate,
        List<Directive> directives) {

    public Scenario {
        name = name == null || name.isBlank() ? "unnamed" : name;
        faults = faults == null ? FaultProfile.none() : faults;
        statusApi = statusApi == null ? StatusApiFaults.none() : statusApi;
        payments = payments == null ? List.of() : List.copyOf(payments);
        directives = directives == null ? List.of() : List.copyOf(directives);
    }

    public Scenario withDefaults(long defaultSeed, String defaultTargetUrl, double defaultTimeScale) {
        return new Scenario(
                name,
                seed == null ? defaultSeed : seed,
                targetUrl == null || targetUrl.isBlank() ? defaultTargetUrl : targetUrl,
                timeScale == null ? defaultTimeScale : timeScale,
                faults, statusApi, payments, generate, directives);
    }
}
