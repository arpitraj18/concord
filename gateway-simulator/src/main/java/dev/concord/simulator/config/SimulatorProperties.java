package dev.concord.simulator.config;

import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

/**
 * @param gateway          which dialect this instance speaks: A, B or C
 * @param webhookSecret    shared secret used to sign webhooks
 * @param apiKey           if set, the status API requires {@code Authorization: Bearer <apiKey>}
 * @param defaultTargetUrl where webhooks go when a scenario does not say
 * @param defaultTimeScale virtual ms per real ms when a scenario does not say
 * @param webhookTimeoutMs per-attempt HTTP timeout (real ms)
 */
@Validated
@ConfigurationProperties(prefix = "simulator")
public record SimulatorProperties(
        @NotBlank String gateway,
        @NotBlank String webhookSecret,
        String apiKey,
        @NotBlank String defaultTargetUrl,
        @Positive @DecimalMax("1000") double defaultTimeScale,
        @Positive long webhookTimeoutMs,
        @DefaultValue Retry retry,
        @DefaultValue DevSink devSink) {

    /**
     * Gateways retry webhooks that do not get a 2xx. Backoff is in virtual ms, so it speeds up with
     * the time scale like everything else.
     */
    public record Retry(
            @DefaultValue("4") @Min(1) int maxAttempts,
            @DefaultValue("2000") @Min(0) long initialBackoffMs,
            @DefaultValue("2.0") double multiplier) {

        public long backoffMs(int failedAttempt) {
            return (long) (initialBackoffMs * Math.pow(multiplier, failedAttempt - 1));
        }
    }

    public record DevSink(@DefaultValue("true") boolean enabled, @DefaultValue("10000") int capacity) {
    }
}
