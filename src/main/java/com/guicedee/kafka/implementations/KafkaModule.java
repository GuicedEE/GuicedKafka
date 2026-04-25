package com.guicedee.kafka.implementations;

import com.google.common.base.Strings;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Singleton;
import com.google.inject.name.Names;
import com.guicedee.client.Environment;
import com.guicedee.client.services.lifecycle.IGuiceModule;
import com.guicedee.kafka.*;
import com.guicedee.vertx.spi.VertXPreStartup;
import io.vertx.kafka.admin.KafkaAdminClient;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.producer.KafkaProducer;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;

import java.util.*;

/**
 * Guice module that binds Kafka producers, consumers, and publishers based on
 * discovered annotations at startup.
 */
@Log4j2
public class KafkaModule extends AbstractModule implements IGuiceModule<KafkaModule>
{
    @Getter
    private static final Map<String, KafkaProducer<String, String>> packageProducers = new HashMap<>();
    @Getter
    private static final Map<String, KafkaConsumer<String, String>> packageConsumers = new HashMap<>();
    @Getter
    private static final Map<String, KafkaAdminClient> adminClients = new HashMap<>();

    @Override
    protected void configure()
    {
        Set<String> completedPublishers = new HashSet<>();

        KafkaPreStartup.getPackageKafkaConnections().forEach((packageName, connections) -> {
            for (KafkaConnectionOptions connectionOption : connections)
            {
                // Create producer config
                Map<String, String> producerConfig = toProducerConfig(connectionOption);
                var producer = KafkaProducer.<String, String>create(VertXPreStartup.getVertx(), producerConfig);
                packageProducers.put(packageName, producer);
                bind(Key.get(KafkaProducer.class, Names.named(connectionOption.value()))).toInstance(producer);

                // Create consumer config
                Map<String, String> consumerConfig = toConsumerConfig(connectionOption);
                var consumer = KafkaConsumer.<String, String>create(VertXPreStartup.getVertx(), consumerConfig);
                packageConsumers.put(packageName, consumer);
                bind(Key.get(KafkaConsumer.class, Names.named(connectionOption.value()))).toInstance(consumer);

                // Create admin client per connection
                Map<String, String> adminConfig = new HashMap<>();
                adminConfig.put("bootstrap.servers", connectionOption.bootstrapServers());
                if (!Strings.isNullOrEmpty(connectionOption.clientId()))
                {
                    adminConfig.put("client.id", connectionOption.clientId() + "-admin");
                }
                KafkaAdminClient adminClient = KafkaAdminClient.create(VertXPreStartup.getVertx(), adminConfig);
                adminClients.put(connectionOption.value(), adminClient);
                bind(Key.get(KafkaAdminClient.class, Names.named(connectionOption.value()))).toInstance(adminClient);
            }
        });

        // Bind consumer classes
        KafkaPreStartup.getTopicConsumerDefinitions().forEach((topicName, topicDef) -> {
            Class clazz = KafkaPreStartup.getTopicConsumerClass().get(topicName);
            bind(clazz).in(Singleton.class);
            bind(Key.get(clazz, Names.named(topicName))).to(clazz);
            bind(Key.get(KafkaTopicConsumer.class, Names.named(topicName))).to(clazz);
        });

        // Bind publishers
        KafkaPreStartup.getPackageKafkaConnections().forEach((packageName, connections) -> {
            for (KafkaConnectionOptions connectionOption : connections)
            {
                var producer = packageProducers.get(packageName);

                KafkaPreStartup.getTopicPublisherDefinitions().forEach((topicName, topicDef) -> {
                    if (!completedPublishers.contains(topicName))
                    {
                        completedPublishers.add(topicName);
                        bind(Key.get(KafkaTopicPublisher.class, Names.named(topicName)))
                                .toProvider(() -> new KafkaTopicPublisher(producer, topicName))
                                .in(Singleton.class);
                    }
                });
            }
        });
    }

    /**
     * Builds the consumer configuration map from annotation options.
     */
    public static Map<String, String> toConsumerConfig(KafkaConnectionOptions options)
    {
        Map<String, String> config = new HashMap<>();
        config.put("bootstrap.servers", options.bootstrapServers());
        config.put("key.deserializer", options.keyDeserializer());
        config.put("value.deserializer", options.valueDeserializer());
        if (!Strings.isNullOrEmpty(options.groupId()))
        {
            config.put("group.id", options.groupId());
        }
        config.put("auto.offset.reset", options.autoOffsetReset());
        config.put("enable.auto.commit", String.valueOf(options.enableAutoCommit()));
        if (options.autoCommitIntervalMs() != 5000)
        {
            config.put("auto.commit.interval.ms", String.valueOf(options.autoCommitIntervalMs()));
        }
        if (options.sessionTimeoutMs() != 10000)
        {
            config.put("session.timeout.ms", String.valueOf(options.sessionTimeoutMs()));
        }
        if (options.maxPollRecords() != 500)
        {
            config.put("max.poll.records", String.valueOf(options.maxPollRecords()));
        }
        if (options.requestTimeoutMs() != 30000)
        {
            config.put("request.timeout.ms", String.valueOf(options.requestTimeoutMs()));
        }
        if (!Strings.isNullOrEmpty(options.clientId()))
        {
            config.put("client.id", options.clientId());
        }
        return config;
    }

    /**
     * Builds the producer configuration map from annotation options.
     */
    public static Map<String, String> toProducerConfig(KafkaConnectionOptions options)
    {
        Map<String, String> config = new HashMap<>();
        config.put("bootstrap.servers", options.bootstrapServers());
        config.put("key.serializer", options.keySerializer());
        config.put("value.serializer", options.valueSerializer());
        config.put("acks", options.acks());
        if (options.retries() != 0)
        {
            config.put("retries", String.valueOf(options.retries()));
        }
        if (options.lingerMs() != 0)
        {
            config.put("linger.ms", String.valueOf(options.lingerMs()));
        }
        if (options.batchSize() != 16384)
        {
            config.put("batch.size", String.valueOf(options.batchSize()));
        }
        if (options.bufferMemory() != 33554432L)
        {
            config.put("buffer.memory", String.valueOf(options.bufferMemory()));
        }
        if (options.requestTimeoutMs() != 30000)
        {
            config.put("request.timeout.ms", String.valueOf(options.requestTimeoutMs()));
        }
        if (!Strings.isNullOrEmpty(options.clientId()))
        {
            config.put("client.id", options.clientId());
        }
        return config;
    }
}



