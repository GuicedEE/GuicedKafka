package com.guicedee.kafka;

import tools.jackson.databind.annotation.JsonSerialize;
import io.vertx.core.Future;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.RecordMetadata;
import lombok.EqualsAndHashCode;
import lombok.extern.log4j.Log4j2;

/**
 * Publishes messages to a Kafka topic. Bound via Guice with {@code @Named("topic-name")}.
 */
@JsonSerialize(as = Void.class)
@EqualsAndHashCode(of = {"topicName"})
@Log4j2
public class KafkaTopicPublisher
{
    private final KafkaProducer<String, String> producer;
    private final String topicName;

    /**
     * Creates a publisher bound to a specific topic.
     *
     * @param producer  The Kafka producer used to send messages.
     * @param topicName The topic to publish to.
     */
    public KafkaTopicPublisher(KafkaProducer<String, String> producer, String topicName)
    {
        this.producer = producer;
        this.topicName = topicName;
    }

    /**
     * Sends a message with only a value (null key), round-robin across partitions.
     *
     * @param value The message value.
     * @return A future with the record metadata.
     */
    public Future<RecordMetadata> send(String value)
    {
        return send(null, value);
    }

    /**
     * Sends a message with a key and value.
     *
     * @param key   The message key (determines partition).
     * @param value The message value.
     * @return A future with the record metadata.
     */
    public Future<RecordMetadata> send(String key, String value)
    {
        KafkaProducerRecord<String, String> record = KafkaProducerRecord.create(topicName, key, value);
        return producer.send(record)
                .onSuccess(metadata -> log.trace("Message sent to topic={}, partition={}, offset={}",
                        metadata.getTopic(), metadata.getPartition(), metadata.getOffset()))
                .onFailure(t -> log.error("Failed to send message to topic {}", topicName, t));
    }

    /**
     * Sends a message to a specific partition.
     *
     * @param key       The message key.
     * @param value     The message value.
     * @param partition The target partition.
     * @return A future with the record metadata.
     */
    public Future<RecordMetadata> send(String key, String value, int partition)
    {
        KafkaProducerRecord<String, String> record = KafkaProducerRecord.create(topicName, key, value, partition);
        return producer.send(record)
                .onSuccess(metadata -> log.trace("Message sent to topic={}, partition={}, offset={}",
                        metadata.getTopic(), metadata.getPartition(), metadata.getOffset()))
                .onFailure(t -> log.error("Failed to send message to topic {} partition {}", topicName, partition, t));
    }

    /**
     * Writes a message (fire-and-forget, no metadata callback).
     *
     * @param value The message value.
     */
    public void write(String value)
    {
        KafkaProducerRecord<String, String> record = KafkaProducerRecord.create(topicName, value);
        producer.write(record);
    }

    /**
     * Writes a keyed message (fire-and-forget).
     *
     * @param key   The message key.
     * @param value The message value.
     */
    public void write(String key, String value)
    {
        KafkaProducerRecord<String, String> record = KafkaProducerRecord.create(topicName, key, value);
        producer.write(record);
    }

    /**
     * Flushes any buffered messages.
     *
     * @return A future that completes when flushed.
     */
    public Future<Void> flush()
    {
        return producer.flush();
    }

    /**
     * @return The topic name this publisher targets.
     */
    public String getTopicName()
    {
        return topicName;
    }
}

