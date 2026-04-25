import com.guicedee.client.services.lifecycle.IGuiceModule;
import com.guicedee.client.services.lifecycle.IGuicePostStartup;
import com.guicedee.client.services.lifecycle.IGuicePreDestroy;
import com.guicedee.client.services.lifecycle.IGuicePreStartup;
import com.guicedee.kafka.implementations.*;

module com.guicedee.kafka {
    exports com.guicedee.kafka;
    exports com.guicedee.kafka.implementations;

    requires transitive org.apache.kafka.client;
    requires transitive io.vertx.client.kafka;
    requires com.guicedee.vertx;
    requires com.guicedee.client;
    requires static lombok;

    requires io.github.classgraph;
    requires org.apache.commons.lang3;

    provides IGuicePostStartup with KafkaPostStartup;
    provides IGuiceModule with KafkaModule;
    provides IGuicePreStartup with KafkaPreStartup;
    provides IGuicePreDestroy with KafkaPreDestroy;

    opens com.guicedee.kafka to com.google.guice, com.fasterxml.jackson.databind;
    opens com.guicedee.kafka.implementations to com.fasterxml.jackson.databind, com.google.guice;
}



