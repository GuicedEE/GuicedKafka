package com.guicedee.kafka.test;

import com.guicedee.kafka.KafkaTopicPublisher;
import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 * Test service that injects a publisher by topic name,
 * validating that field-based publisher discovery works.
 */
public class TestPublisherService
{
    @Inject
    @Named("test-topic")
    private KafkaTopicPublisher testTopicPublisher;

    @Inject
    @Named("keyed-topic")
    private KafkaTopicPublisher keyedTopicPublisher;

    @Inject
    @Named("admin-created-topic")
    private KafkaTopicPublisher adminCreatedPublisher;

    public KafkaTopicPublisher getTestTopicPublisher()
    {
        return testTopicPublisher;
    }

    public KafkaTopicPublisher getKeyedTopicPublisher()
    {
        return keyedTopicPublisher;
    }

    public KafkaTopicPublisher getAdminCreatedPublisher()
    {
        return adminCreatedPublisher;
    }
}

