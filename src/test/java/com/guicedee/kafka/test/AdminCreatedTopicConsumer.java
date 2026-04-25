package com.guicedee.kafka.test;

import com.guicedee.kafka.KafkaTopicConsumer;
import com.guicedee.kafka.KafkaTopicCreate;
import com.guicedee.kafka.KafkaTopicDefinition;
import com.guicedee.kafka.KafkaTopicOptions;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

/**
 * Consumer for a topic that is created at startup via @KafkaTopicCreate.
 */
@KafkaTopicCreate(value = "admin-created-topic", partitions = 2, replicationFactor = 1)
@KafkaTopicDefinition(
        value = "admin-created-topic",
        options = @KafkaTopicOptions(worker = true)
)
public class AdminCreatedTopicConsumer implements KafkaTopicConsumer<String, String>
{
    private static final CopyOnWriteArrayList<String> receivedMessages = new CopyOnWriteArrayList<>();
    private static volatile CountDownLatch latch = new CountDownLatch(1);

    public static void setLatch(CountDownLatch newLatch)
    {
        latch = newLatch;
    }

    public static List<String> getReceivedMessages()
    {
        return receivedMessages;
    }

    public static void clearMessages()
    {
        receivedMessages.clear();
    }

    @Override
    public void consume(KafkaConsumerRecord<String, String> record)
    {
        System.out.println("Admin-created topic consumer - key=" + record.key() + ", value=" + record.value()
                + ", partition=" + record.partition());
        receivedMessages.add(record.value());
        latch.countDown();
    }
}

