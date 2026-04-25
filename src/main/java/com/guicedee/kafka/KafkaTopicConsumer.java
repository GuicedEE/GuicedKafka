package com.guicedee.kafka;

import io.vertx.kafka.client.consumer.KafkaConsumerRecord;

/**
 * Contract for consuming messages from a Kafka topic.
 *
 * @param <K> The key type.
 * @param <V> The value type.
 */
public interface KafkaTopicConsumer<K, V>
{
    /**
     * Handles a single Kafka consumer record.
     *
     * @param record The received record containing key, value, partition, and offset.
     */
    void consume(KafkaConsumerRecord<K, V> record);
}

