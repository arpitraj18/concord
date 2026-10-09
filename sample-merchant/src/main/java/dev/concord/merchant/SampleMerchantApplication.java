package dev.concord.merchant;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Configuration lives in {@code sample-merchant.yml}, not {@code application.yml}: when several Spring
 * Boot apps share one classpath (the system tests), Spring loads whichever {@code application.yml} it
 * finds first, and this app would silently get another app's settings.
 */
@SpringBootApplication
public class SampleMerchantApplication {

    public static final String CONFIG_NAME = "sample-merchant";

    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(SampleMerchantApplication.class);
        app.setDefaultProperties(java.util.Map.of("spring.config.name", CONFIG_NAME));
        app.run(args);
    }
}
