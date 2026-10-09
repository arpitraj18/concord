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
        try {
            postgres = EmbeddedPostgres.start();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        // Command-line arguments, not builder.properties(): those are only defaults (see what-broke.md).
        app = new SpringApplicationBuilder(SampleMerchantApplication.class).run(
                "--spring.config.name=" + SampleMerchantApplication.CONFIG_NAME,
                "--server.port=0",
                "--spring.datasource.url=" + postgres.getJdbcUrl("postgres", "postgres"),
                "--spring.datasource.username=postgres",
                "--spring.datasource.password=postgres",
                "--merchant.concord-secret=" + concordSecret,
                "--spring.main.banner-mode=off");
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
