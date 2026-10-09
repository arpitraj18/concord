package dev.concord.merchant;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;

/** The sample merchant on a real PostgreSQL (embedded binaries), for tests in this and later modules. */
public final class EmbeddedMerchant implements AutoCloseable {

    private final EmbeddedPostgres postgres;
    private final ConfigurableApplicationContext app;

    public EmbeddedMerchant(String concordSecret) {
        this(concordSecret, new String[0]);
    }

    /** @param extraArgs more command-line settings, e.g. "--merchant.checkout-timeout-ms=2000" */
    public EmbeddedMerchant(String concordSecret, String... extraArgs) {
        try {
            postgres = EmbeddedPostgres.start();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        // Command-line arguments, not builder.properties(): those are only defaults (see what-broke.md).
        java.util.List<String> args = new java.util.ArrayList<>(java.util.List.of(
                "--spring.config.name=" + SampleMerchantApplication.CONFIG_NAME,
                "--server.port=0",
                "--spring.datasource.url=" + postgres.getJdbcUrl("postgres", "postgres"),
                "--spring.datasource.username=postgres",
                "--spring.datasource.password=postgres",
                "--merchant.concord-secret=" + concordSecret,
                "--spring.main.banner-mode=off"));
        args.addAll(java.util.List.of(extraArgs));
        app = new SpringApplicationBuilder(SampleMerchantApplication.class).run(args.toArray(String[]::new));
    }

    public String baseUrl() {
        return "http://127.0.0.1:" + app.getEnvironment().getProperty("local.server.port");
    }

    public JdbcTemplate jdbc() {
        return app.getBean(JdbcTemplate.class);
    }

    public OrderService orders() {
        return app.getBean(OrderService.class);
    }

    public Map<String, Object> stats() {
        return orders().stats();
    }

    @Override
    public void close() {
        app.close();
        try {
            postgres.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
