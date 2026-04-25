package com.guicedee.kafka.test;

import com.guicedee.kafka.KafkaTopicConsumer;
import com.guicedee.kafka.KafkaTopicDefinition;
import com.guicedee.kafka.KafkaTopicOptions;
import com.google.inject.Singleton;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.List;

@KafkaTopicDefinition(
        value = "test-topic",
        options = @KafkaTopicOptions(worker = true)
)
@Singleton
public class TestTopicConsumer implements KafkaTopicConsumer<String, String>
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
        System.out.println("Consumed - key=" + record.key() + ", value=" + record.value()
                + ", partition=" + record.partition() + ", offset=" + record.offset());
        receivedMessages.add(record.value());
        latch.countDown();
    }
}


