package dev.concord.simulator.config;

import dev.concord.simulator.dialect.Dialects;
import dev.concord.simulator.dialect.GatewayDialect;
import dev.concord.simulator.plan.DeliveryPlanner;
import java.net.http.HttpClient;
import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(SimulatorProperties.class)
public class SimulatorConfig {

    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    public GatewayDialect gatewayDialect(SimulatorProperties props) {
        return Dialects.forId(props.gateway());
    }

    @Bean
    public DeliveryPlanner deliveryPlanner(GatewayDialect dialect) {
        return new DeliveryPlanner(dialect);
    }

    /** Only decides <em>when</em> to send; the blocking HTTP call runs on a virtual thread. */
    @Bean(destroyMethod = "shutdownNow")
    public ScheduledExecutorService webhookScheduler() {
        return Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().name("webhook-scheduler").factory());
    }

    @Bean
    public HttpClient webhookHttpClient() {
        return HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(2))
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .build();
    }
}
