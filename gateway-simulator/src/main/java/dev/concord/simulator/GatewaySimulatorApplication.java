package dev.concord.simulator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

/** Holds no data of its own: never auto-configure a database, even if a JDBC driver is on the classpath. */
@SpringBootApplication(exclude = DataSourceAutoConfiguration.class)
public class GatewaySimulatorApplication {

    public static void main(String[] args) {
        SpringApplication.run(GatewaySimulatorApplication.class, args);
    }
}
