package com.guicedee.kafka.test;

import com.guicedee.client.IGuiceContext;
import com.guicedee.kafka.KafkaTopicPublisher;
import com.guicedee.kafka.implementations.KafkaPreStartup;
import com.google.inject.Key;
import com.google.inject.name.Names;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class KafkaPostStartupTest
{
    private static org.testcontainers.containers.KafkaContainer kafkaContainer;

    @BeforeAll
    static void startKafka()
    {
        kafkaContainer = new org.testcontainers.containers.KafkaContainer(
                org.testcontainers.utility.DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));
        kafkaContainer.start();

        // Set the bootstrap servers so the annotation env override picks it up
        System.setProperty("KAFKA_BOOTSTRAP_SERVERS", kafkaContainer.getBootstrapServers());
    }

    @AfterAll
    static void stopKafka()
    {
        if (kafkaContainer != null)
        {
            kafkaContainer.stop();
        }
        System.clearProperty("KAFKA_BOOTSTRAP_SERVERS");
    }

    @Test
    @Order(1)
    void testProduceAndConsumeMessage() throws Exception
    {
        TestTopicConsumer.clearMessages();
        CountDownLatch latch = new CountDownLatch(1);
        TestTopicConsumer.setLatch(latch);

        IGuiceContext.instance()
                .getConfig()
                .setClasspathScanning(true)
                .setAnnotationScanning(true)
                .setFieldScanning(true);

        IGuiceContext.instance().inject();

        // Get the publisher
        KafkaTopicPublisher publisher = IGuiceContext.get(Key.get(KafkaTopicPublisher.class, Names.named("test-topic")));
        assertNotNull(publisher, "Publisher should be bound");

        // Send a message
        publisher.send("test-key", "Hello Kafka from GuicedEE!").onComplete(ar -> {
            if (ar.succeeded())
            {
                System.out.println("Message sent: topic=" + ar.result().getTopic()
                        + ", partition=" + ar.result().getPartition()
                        + ", offset=" + ar.result().getOffset());
            }
            else
            {
                System.err.println("Send failed: " + ar.cause().getMessage());
            }
        });

        // Wait for consumer to receive the message
        boolean received = latch.await(30, TimeUnit.SECONDS);
        assertTrue(received, "Consumer should have received the message within 30 seconds");

        List<String> messages = TestTopicConsumer.getReceivedMessages();
        assertFalse(messages.isEmpty(), "Should have received at least one message");
        assertEquals("Hello Kafka from GuicedEE!", messages.get(0));
    }

    @Test
    @Order(2)
    void testMultipleMessages() throws Exception
    {
        TestTopicConsumer.clearMessages();
        int messageCount = 5;
        CountDownLatch latch = new CountDownLatch(messageCount);
        TestTopicConsumer.setLatch(latch);

        KafkaTopicPublisher publisher = IGuiceContext.get(Key.get(KafkaTopicPublisher.class, Names.named("test-topic")));

        for (int i = 0; i < messageCount; i++)
        {
            publisher.send("key-" + i, "message_" + i);
        }

        boolean allReceived = latch.await(30, TimeUnit.SECONDS);
        assertTrue(allReceived, "All " + messageCount + " messages should be received");
        assertEquals(messageCount, TestTopicConsumer.getReceivedMessages().size());
    }

    @Test
    @Order(3)
    void testKeyedTopicConsumer() throws Exception
    {
        KeyedTopicConsumer.clearMessages();
        CountDownLatch latch = new CountDownLatch(1);
        KeyedTopicConsumer.setLatch(latch);

        KafkaTopicPublisher publisher = IGuiceContext.get(Key.get(KafkaTopicPublisher.class, Names.named("keyed-topic")));
        assertNotNull(publisher, "Keyed topic publisher should be bound");

        publisher.send("my-key", "keyed-value");

        boolean received = latch.await(30, TimeUnit.SECONDS);
        assertTrue(received, "Keyed consumer should have received the message");

        List<String> messages = KeyedTopicConsumer.getReceivedMessages();
        assertFalse(messages.isEmpty());
        assertEquals("my-key:keyed-value", messages.get(0));
    }

    @Test
    @Order(4)
    void testFireAndForgetWrite() throws Exception
    {
        TestTopicConsumer.clearMessages();
        CountDownLatch latch = new CountDownLatch(1);
        TestTopicConsumer.setLatch(latch);

        KafkaTopicPublisher publisher = IGuiceContext.get(Key.get(KafkaTopicPublisher.class, Names.named("test-topic")));
        publisher.write("fire-and-forget-value");

        boolean received = latch.await(30, TimeUnit.SECONDS);
        assertTrue(received, "Fire-and-forget message should be received");
        assertTrue(TestTopicConsumer.getReceivedMessages().contains("fire-and-forget-value"));
    }

    @Test
    @Order(5)
    void testPublisherServiceInjection()
    {
        // Verify that the TestPublisherService gets its publishers injected
        TestPublisherService service = IGuiceContext.get(TestPublisherService.class);
        assertNotNull(service, "Service should be injectable");
        assertNotNull(service.getTestTopicPublisher(), "Test topic publisher should be injected");
        assertNotNull(service.getKeyedTopicPublisher(), "Keyed topic publisher should be injected");
        assertNotNull(service.getAdminCreatedPublisher(), "Admin-created topic publisher should be injected");
    }

    @Test
    @Order(6)
    void testAdminCreatedTopicConsumer() throws Exception
    {
        AdminCreatedTopicConsumer.clearMessages();
        CountDownLatch latch = new CountDownLatch(1);
        AdminCreatedTopicConsumer.setLatch(latch);

        KafkaTopicPublisher publisher = IGuiceContext.get(Key.get(KafkaTopicPublisher.class, Names.named("admin-created-topic")));
        assertNotNull(publisher, "Admin-created topic publisher should be bound");

        publisher.send("admin-key", "admin-value");

        boolean received = latch.await(30, TimeUnit.SECONDS);
        assertTrue(received, "Admin-created topic consumer should have received the message");

        List<String> messages = AdminCreatedTopicConsumer.getReceivedMessages();
        assertFalse(messages.isEmpty());
        assertEquals("admin-value", messages.get(0));
    }

    @Test
    @Order(7)
    void testPackageInfoConnectionDiscovery()
    {
        // Verify that package-level connections were discovered
        assertFalse(KafkaPreStartup.getPackageKafkaConnections().isEmpty(),
                "Should have discovered at least one package-level connection");

        // Verify the connection name matches what's in package-info.java
        boolean foundTestConnection = KafkaPreStartup.getPackageKafkaConnections().values().stream()
                .flatMap(List::stream)
                .anyMatch(conn -> "test-connection".equals(conn.value()));
        assertTrue(foundTestConnection, "Should have found 'test-connection' from package-info.java");
    }
}

