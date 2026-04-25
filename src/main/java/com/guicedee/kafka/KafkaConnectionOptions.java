package com.guicedee.kafka;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares connection-level configuration for a Kafka cluster.
 * Place on a class or {@code package-info.java}.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.PACKAGE})
public @interface KafkaConnectionOptions
{
    /**
     * @return Logical name of this connection for diagnostics and bindings.
     */
    String value() default "default";

    /**
     * @return Comma-separated list of bootstrap servers (host:port).
     */
    String bootstrapServers() default "localhost:9092";

    /**
     * @return The consumer group id. Empty string means no group.
     */
    String groupId() default "";

    /**
     * @return Key deserializer class name for consumers.
     */
    String keyDeserializer() default "org.apache.kafka.common.serialization.StringDeserializer";

    /**
     * @return Value deserializer class name for consumers.
     */
    String valueDeserializer() default "org.apache.kafka.common.serialization.StringDeserializer";

    /**
     * @return Key serializer class name for producers.
     */
    String keySerializer() default "org.apache.kafka.common.serialization.StringSerializer";

    /**
     * @return Value serializer class name for producers.
     */
    String valueSerializer() default "org.apache.kafka.common.serialization.StringSerializer";

    /**
     * @return Auto offset reset strategy: "earliest", "latest", or "none".
     */
    String autoOffsetReset() default "earliest";

    /**
     * @return Whether to enable auto-commit for consumers.
     */
    boolean enableAutoCommit() default false;

    /**
     * @return Auto-commit interval in milliseconds.
     */
    int autoCommitIntervalMs() default 5000;

    /**
     * @return Producer acknowledgement mode: "0", "1", or "all".
     */
    String acks() default "1";

    /**
     * @return Number of retries for producer sends.
     */
    int retries() default 0;

    /**
     * @return Linger time in milliseconds for producer batching.
     */
    int lingerMs() default 0;

    /**
     * @return Producer batch size in bytes.
     */
    int batchSize() default 16384;

    /**
     * @return Producer buffer memory in bytes.
     */
    long bufferMemory() default 33554432L;

    /**
     * @return Request timeout in milliseconds.
     */
    int requestTimeoutMs() default 30000;

    /**
     * @return Session timeout in milliseconds for consumer group management.
     */
    int sessionTimeoutMs() default 10000;

    /**
     * @return Maximum poll records for consumers.
     */
    int maxPollRecords() default 500;

    /**
     * @return Client id for Kafka connections.
     */
    String clientId() default "";
}

