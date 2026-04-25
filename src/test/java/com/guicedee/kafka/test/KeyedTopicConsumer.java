package com.guicedee.kafka.test;

import com.guicedee.kafka.KafkaTopicConsumer;
import com.guicedee.kafka.KafkaTopicDefinition;
import com.guicedee.kafka.KafkaTopicOptions;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.List;

@KafkaTopicDefinition(
        value = "keyed-topic",
        options = @KafkaTopicOptions(autoCommit = true)
)
public class KeyedTopicConsumer implements KafkaTopicConsumer<String, String>
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
        System.out.println("Keyed consumer - key=" + record.key() + ", value=" + record.value());
        receivedMessages.add(record.key() + ":" + record.value());
        latch.countDown();
    }
}

