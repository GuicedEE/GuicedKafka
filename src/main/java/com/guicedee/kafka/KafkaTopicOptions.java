package com.guicedee.kafka;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Tuning options for a Kafka topic consumer.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface KafkaTopicOptions
{
    /**
     * @return Whether to auto-commit offsets for this consumer.
     */
    boolean autoCommit() default false;

    /**
     * @return Whether to process messages on a worker thread (blocking allowed).
     */
    boolean worker() default false;

    /**
     * @return Number of consumer instances (verticle instances) to deploy.
     */
    int consumerCount() default 1;

    /**
     * @return Specific partition to assign (-1 for group-based subscription).
     */
    int partition() default -1;

    /**
     * @return Maximum poll interval in milliseconds (0 for default).
     */
    int maxPollIntervalMs() default 0;

    /**
     * @return Whether to pause the consumer on startup and require manual resume.
     */
    boolean pauseOnStart() default false;
}

