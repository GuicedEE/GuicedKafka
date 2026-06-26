module com.guicedee.kafka.test {
    requires com.guicedee.kafka;
    requires com.guicedee.client;
    requires com.guicedee.guicedinjection;
    requires com.google.guice;

    requires org.junit.jupiter.api;
    requires org.testcontainers;
    requires org.apache.commons.lang3;

    requires io.vertx.client.kafka;

    opens com.guicedee.kafka.test to com.google.guice, tools.jackson.databind, io.github.classgraph, org.junit.platform.commons;
}



