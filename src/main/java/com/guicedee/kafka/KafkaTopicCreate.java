package com.guicedee.kafka;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares a Kafka topic to be created at startup via the Admin Client.
 * Place on a class or {@code package-info.java} alongside {@link KafkaConnectionOptions}.
 * Repeatable — multiple topics can be declared on a single element.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.PACKAGE})
@Repeatable(KafkaTopicCreates.class)
public @interface KafkaTopicCreate
{
    /**
     * @return The topic name to create.
     */
    String value();

    /**
     * @return Number of partitions for the topic.
     */
    int partitions() default 1;

    /**
     * @return Replication factor for the topic.
     */
    short replicationFactor() default 1;

    /**
     * @return Whether to ignore errors if the topic already exists.
     */
    boolean ignoreIfExists() default true;
}


