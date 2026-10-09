package dev.concord.merchant;

import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** Cancels orders still awaiting payment after {@code merchant.checkout-timeout-ms} (0 = off). */
@Component
@EnableScheduling
public class CheckoutTimeoutJob {

    private static final Logger log = LoggerFactory.getLogger(CheckoutTimeoutJob.class);

    private final OrderService orders;
    private final Duration timeout;

    public CheckoutTimeoutJob(OrderService orders, @Value("${merchant.checkout-timeout-ms}") long timeoutMs) {
        this.orders = orders;
        this.timeout = Duration.ofMillis(timeoutMs);
    }

    @Scheduled(fixedDelayString = "${merchant.timeout-check-interval-ms}")
    public void run() {
        if (timeout.isZero()) {
            return;
        }
        int cancelled = orders.cancelTimedOut(timeout);
        if (cancelled > 0) {
            log.info("checkout timeout: cancelled {} unpaid orders", cancelled);
        }
    }
}
