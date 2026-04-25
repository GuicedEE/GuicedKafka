package com.guicedee.kafka.implementations;

import com.guicedee.kafka.KafkaTopicOptions;

import java.lang.annotation.Annotation;

/**
 * Default mutable implementation of {@link KafkaTopicOptions} used when no annotation is present.
 */
public class KafkaTopicOptionsDefault implements KafkaTopicOptions
{
    @Override
    public Class<? extends Annotation> annotationType() { return KafkaTopicOptions.class; }

    @Override
    public boolean autoCommit() { return false; }

    @Override
    public boolean worker() { return false; }

    @Override
    public int consumerCount() { return 1; }

    @Override
    public int partition() { return -1; }

    @Override
    public int maxPollIntervalMs() { return 0; }

    @Override
    public boolean pauseOnStart() { return false; }

    @Override
    public boolean equals(Object obj) { return true; }

    @Override
    public int hashCode() { return "0".hashCode(); }
}

