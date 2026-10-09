package dev.concord.simulator.run;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.concord.simulator.config.SimulatorProperties;
import dev.concord.simulator.dialect.GatewayDialect;
import dev.concord.simulator.plan.PaymentTruth;
import dev.concord.simulator.plan.PlannedDelivery;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Executes a run's plan: sends each webhook at its planned time and retries on non-2xx like a real
 * gateway. The plan is deterministic; retries are not, because they react to the receiver.
 */
@Component
public class WebhookDispatcher {

    private static final Logger log = LoggerFactory.getLogger(WebhookDispatcher.class);

    private final GatewayDialect dialect;
    private final SimulatorProperties props;
    private final ScheduledExecutorService scheduler;
    private final HttpClient http;
    private final ObjectMapper json;
    private final Clock clock;

    public WebhookDispatcher(GatewayDialect dialect, SimulatorProperties props, ScheduledExecutorService webhookScheduler,
                             HttpClient webhookHttpClient, ObjectMapper json, Clock clock) {
        this.dialect = dialect;
        this.props = props;
        this.scheduler = webhookScheduler;
        this.http = webhookHttpClient;
        this.json = json;
        this.clock = clock;
    }

    public boolean start(SimulationRun run) {
        var toSend = run.plan().toSend();
        if (!run.markStarted(clock.instant(), toSend.size())) {
            return false;
        }
        Map<String, PaymentTruth> payments = run.plan().paymentsById();
        for (PlannedDelivery d : toSend) {
            PaymentTruth payment = payments.get(d.paymentId());
            schedule(run, payment, d, 1, run.realDelayMs(d.deliverAtMs()));
        }
        log.info("run {} started: {} webhooks to {} (timeScale {})", run.id(), toSend.size(), run.targetUrl(), run.timeScale());
        return true;
    }

    private void schedule(SimulationRun run, PaymentTruth payment, PlannedDelivery d, int attempt, long realDelayMs) {
        run.track(scheduler.schedule(
                () -> Thread.startVirtualThread(() -> attempt(run, payment, d, attempt)),
                realDelayMs, TimeUnit.MILLISECONDS));
    }

    /** Sends the webhook for a merchant action right away (no injected faults; retries still apply). */
    public void sendNow(SimulationRun run, PaymentTruth payment, dev.concord.simulator.model.SimState state, long virtualNow) {
        String eventId = "evt_act_" + java.util.UUID.randomUUID().toString().replace("-", "").substring(0, 16);
        PlannedDelivery d = new PlannedDelivery(-actionSeq.incrementAndGet(), payment.paymentId(), state,
                dialect.eventType(state), eventId, PlannedDelivery.Kind.ORIGINAL, false, virtualNow, virtualNow, "merchant-action");
        run.addDelivery();
        schedule(run, payment, d, 1, 0);
    }

    private final java.util.concurrent.atomic.AtomicInteger actionSeq = new java.util.concurrent.atomic.AtomicInteger();

    private void attempt(SimulationRun run, PaymentTruth payment, PlannedDelivery d, int attempt) {
        if (!run.isActive()) {
            return;
        }
        Long actedAt = run.actedAt(d.paymentId());
        if (actedAt != null && d.seq() >= 0 && d.happenedAtMs() > actedAt && d.kind() != PlannedDelivery.Kind.CONTRADICTION) {
            // A merchant action rewrote this payment's future: the planned step no longer happens.
            run.deliveryFinished(clock.instant());
            return;
        }
        Instant sentAt = clock.instant();
        Integer status = null;
        String error = null;
        try {
            byte[] body = renderBody(run, payment, d);
            HttpRequest.Builder req = HttpRequest.newBuilder(URI.create(run.targetUrl()))
                    .timeout(Duration.ofMillis(props.webhookTimeoutMs()))
                    .header("Content-Type", "application/json")
                    .header("User-Agent", "concord-gateway-simulator/" + dialect.id())
                    .POST(HttpRequest.BodyPublishers.ofByteArray(body));
            dialect.webhookHeaders(props.webhookSecret(), body, d.eventId(), sentAt).forEach(req::header);
            status = http.send(req.build(), HttpResponse.BodyHandlers.discarding()).statusCode();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            error = "interrupted";
        } catch (Exception e) {
            error = e.getClass().getSimpleName() + (e.getMessage() == null ? "" : ": " + e.getMessage());
        }
        long latency = Duration.between(sentAt, clock.instant()).toMillis();
        DeliveryAttempt result = new DeliveryAttempt(d.seq(), attempt, d.paymentId(), d.eventType(), d.eventId(),
                d.kind().name(), sentAt, status, error, latency);
        run.record(result);

        if (!result.succeeded() && attempt < props.retry().maxAttempts() && run.isActive()) {
            schedule(run, payment, d, attempt + 1, run.realDelayMs(props.retry().backoffMs(attempt)));
        } else {
            run.deliveryFinished(clock.instant());
        }
    }

    /** Body bytes are rendered once per attempt from the plan; only times depend on when the run started. */
    byte[] renderBody(SimulationRun run, PaymentTruth payment, PlannedDelivery d) throws JsonProcessingException {
        var ctx = new GatewayDialect.WebhookContext(payment, d.claimedState(), d.eventType(), d.eventId(),
                run.realTimeOf(d.happenedAtMs()), run.startedAt());
        return json.writeValueAsBytes(dialect.webhookBody(ctx));
    }
}
