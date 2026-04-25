package com.guicedee.kafka;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares a Kafka topic for consumption. Place on a class that implements {@link KafkaTopicConsumer}.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.FIELD, ElementType.PACKAGE})
public @interface KafkaTopicDefinition
{
    /**
     * @return The topic name to subscribe to or produce to.
     */
    String value();

    /**
     * @return Topic options for this definition.
     */
    KafkaTopicOptions options() default @KafkaTopicOptions;
}


