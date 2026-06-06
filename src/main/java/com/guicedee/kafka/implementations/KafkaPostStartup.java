package com.guicedee.kafka.implementations;

import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.name.Names;
import com.guicedee.client.IGuiceContext;
import com.guicedee.client.scopes.CallScoper;
import com.guicedee.client.scopes.CallScopeProperties;
import com.guicedee.client.scopes.CallScopeSource;
import com.guicedee.client.services.lifecycle.IGuicePostStartup;
import com.guicedee.kafka.*;
import com.guicedee.vertx.spi.VertXPreStartup;
import io.smallrye.mutiny.Uni;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.kafka.admin.KafkaAdminClient;
import io.vertx.kafka.admin.NewTopic;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;

import java.util.*;

/**
 * Post-startup initializer that creates topics via the admin client,
 * subscribes consumers to topics, and starts consuming.
 */
@Log4j2
public class KafkaPostStartup implements IGuicePostStartup<KafkaPostStartup>
{
    @Inject
    private Vertx vertx;

    @Getter
    private static final Map<String, KafkaConsumer<String, String>> topicConsumers = new HashMap<>();

    @Getter
    private static final Map<String, KafkaAdminClient> adminClients = new HashMap<>();

    @Override
    public List<Uni<Boolean>> postLoad()
    {
        return List.of(Uni.createFrom().item(() -> {
            // First, create any declared topics via the admin client
            createDeclaredTopics();

            // Then start consumers
            KafkaPreStartup.getPackageKafkaConnections().forEach((packageName, connections) -> {
                for (KafkaConnectionOptions connectionOption : connections)
                {
                    KafkaPreStartup.getTopicConsumerDefinitions().forEach((topicName, topicDef) -> {
                        String topicConnectionName = KafkaPreStartup.getTopicConnectionNames().get(topicName);
                        if (topicConnectionName != null && topicConnectionName.equals(connectionOption.value()))
                        {
                            startConsumer(packageName, connectionOption, topicName, topicDef);
                        }
                    });
                }
            });
            // Handle any consumers mapped to default connection
            KafkaPreStartup.getTopicConsumerDefinitions().forEach((topicName, topicDef) -> {
                if (!topicConsumers.containsKey(topicName))
                {
                    // Use any available connection
                    KafkaPreStartup.getPackageKafkaConnections().entrySet().stream().findFirst().ifPresent(entry -> {
                        startConsumer(entry.getKey(), entry.getValue().get(0), topicName, topicDef);
                    });
                }
            });
            return true;
        }));
    }

    /**
     * Creates topics declared via @KafkaTopicCreate annotations using the Kafka Admin Client.
     */
    private void createDeclaredTopics()
    {
        if (KafkaPreStartup.getTopicCreateDefinitions().isEmpty())
        {
            return;
        }

        // Group topic creations by connection
        Map<String, List<NewTopic>> topicsByConnection = new HashMap<>();
        KafkaPreStartup.getTopicCreateDefinitions().forEach((topicName, create) -> {
            String connectionName = KafkaPreStartup.getTopicConnectionNames().getOrDefault(topicName, "default");
            topicsByConnection.computeIfAbsent(connectionName, k -> new ArrayList<>())
                    .add(new NewTopic(topicName, create.partitions(), create.replicationFactor()));
        });

        // Create an admin client per connection and create topics
        KafkaPreStartup.getPackageKafkaConnections().forEach((packageName, connections) -> {
            for (KafkaConnectionOptions connectionOption : connections)
            {
                List<NewTopic> topics = topicsByConnection.get(connectionOption.value());
                if (topics != null && !topics.isEmpty())
                {
                    Map<String, String> adminConfig = new HashMap<>();
                    adminConfig.put("bootstrap.servers", connectionOption.bootstrapServers());
                    if (!connectionOption.clientId().isEmpty())
                    {
                        adminConfig.put("client.id", connectionOption.clientId() + "-admin");
                    }

                    KafkaAdminClient adminClient = KafkaAdminClient.create(VertXPreStartup.getVertx(), adminConfig);
                    adminClients.put(connectionOption.value(), adminClient);

                    adminClient.createTopics(topics)
                            .onSuccess(v -> {
                                for (NewTopic topic : topics)
                                {
                                    log.info("Topic '{}' created (partitions={}, replication={}).",
                                            topic.getName(), topic.getNumPartitions(), topic.getReplicationFactor());
                                }
                            })
                            .onFailure(cause -> {
                                // Check if it's a "topic already exists" error
                                String msg = cause.getMessage();
                                if (msg != null && msg.contains("TopicExistsException"))
                                {
                                    log.debug("Some topics already exist, continuing: {}", msg);
                                }
                                else
                                {
                                    log.error("Failed to create topics: {}", msg, cause);
                                }
                            });
                }
            }
        });
    }

    @SuppressWarnings({"rawtypes"})
    private void startConsumer(String packageName, KafkaConnectionOptions connectionOption, String topicName, KafkaTopicDefinition topicDef)
    {
        Class<? extends KafkaTopicConsumer> consumerClass = KafkaPreStartup.getTopicConsumerClass().get(topicName);
        if (consumerClass == null)
        {
            log.warn("No consumer class found for topic '{}', skipping subscription.", topicName);
            return;
        }

        KafkaTopicOptions options = topicDef.options();

        // Create a dedicated consumer for each topic subscription
        Map<String, String> consumerConfig = KafkaModule.toConsumerConfig(connectionOption);
        if (options.maxPollIntervalMs() > 0)
        {
            consumerConfig.put("max.poll.interval.ms", String.valueOf(options.maxPollIntervalMs()));
        }

        KafkaConsumer<String, String> consumer = KafkaConsumer.create(VertXPreStartup.getVertx(), consumerConfig);
        topicConsumers.put(topicName, consumer);

        // Set up message handler
        consumer.handler(record -> processMessage(record, topicName, topicDef, consumerClass, options));

        // Set up error handler
        consumer.exceptionHandler(e -> log.error("Kafka consumer error for topic '{}': {}", topicName, e.getMessage(), e));

        // Set up partition assigned/revoked handlers
        consumer.partitionsAssignedHandler(partitions -> {
            log.info("Partitions assigned for topic '{}': {}", topicName, partitions);
            for (TopicPartition tp : partitions)
            {
                log.debug("  Assigned: topic={}, partition={}", tp.getTopic(), tp.getPartition());
            }
        });

        consumer.partitionsRevokedHandler(partitions -> {
            log.info("Partitions revoked for topic '{}': {}", topicName, partitions);
            for (TopicPartition tp : partitions)
            {
                log.debug("  Revoked: topic={}, partition={}", tp.getTopic(), tp.getPartition());
            }
        });

        // Subscribe or assign partition
        if (options.partition() >= 0)
        {
            TopicPartition tp = new TopicPartition(topicName, options.partition());
            consumer.assign(Set.of(tp))
                    .onSuccess(v -> {
                        log.info("Kafka consumer assigned to topic '{}' partition {}", topicName, options.partition());
                        if (options.pauseOnStart())
                        {
                            consumer.pause(Set.of(tp));
                            log.info("Kafka consumer for topic '{}' paused on start.", topicName);
                        }
                    })
                    .onFailure(cause -> log.error("Failed to assign partition for topic '{}': {}", topicName, cause.getMessage()));
        }
        else
        {
            consumer.subscribe(Set.of(topicName))
                    .onSuccess(v -> {
                        log.info("Kafka consumer subscribed to topic '{}'", topicName);
                        if (options.pauseOnStart())
                        {
                            log.info("Kafka consumer for topic '{}' paused on start.", topicName);
                        }
                    })
                    .onFailure(cause -> log.error("Failed to subscribe to topic '{}': {}", topicName, cause.getMessage()));
        }
    }

    @SuppressWarnings({"rawtypes"})
    private void processMessage(KafkaConsumerRecord<String, String> record, String topicName,
                                KafkaTopicDefinition topicDef, Class<? extends KafkaTopicConsumer> consumerClass,
                                KafkaTopicOptions options)
    {
        var verticleOptional = VertXPreStartup.getAssociatedVerticle(consumerClass);
        Vertx vertx;
        if (verticleOptional.isEmpty())
        {
            vertx = VertXPreStartup.getVertx();
        }
        else
        {
            vertx = verticleOptional.get().getVertx();
        }

        if (options.worker())
        {
            vertx.executeBlocking(() -> {
                executeConsumer(record, topicName, topicDef, consumerClass, options);
                return true;
            }, false);
        }
        else
        {
            executeConsumer(record, topicName, topicDef, consumerClass, options);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void executeConsumer(KafkaConsumerRecord<String, String> record, String topicName,
                                 KafkaTopicDefinition topicDef, Class<? extends KafkaTopicConsumer> consumerClass,
                                 KafkaTopicOptions options)
    {
        CallScoper scopedRunner = null;
        boolean started = false;
        try
        {
            scopedRunner = IGuiceContext.get(CallScoper.class);
            if (!scopedRunner.isStartedScope())
            {
                scopedRunner.enter();
                started = true;
            }

            var properties = IGuiceContext.get(CallScopeProperties.class);
            properties.setSource(CallScopeSource.Kafka);

            log.trace("Processing Kafka message from topic='{}', partition={}, offset={}, key={}",
                    topicName, record.partition(), record.offset(), record.key());

            var consumer = IGuiceContext.get(Key.get(KafkaTopicConsumer.class, Names.named(topicName)));
            consumer.consume(record);

            // Manual commit if auto-commit is disabled
            if (!options.autoCommit())
            {
                KafkaConsumer<String, String> kafkaConsumer = topicConsumers.get(topicName);
                if (kafkaConsumer != null)
                {
                    kafkaConsumer.commit()
                            .onSuccess(v -> log.trace("Offset committed for topic '{}' partition {} offset {}",
                                    topicName, record.partition(), record.offset()))
                            .onFailure(t -> log.error("Failed to commit offset for topic '{}'", topicName, t));
                }
            }
        }
        catch (Throwable t)
        {
            log.error("Error processing Kafka message for topic '{}'", topicName, t);
        }
        finally
        {
            if (started && scopedRunner != null)
            {
                scopedRunner.exit();
            }
        }
    }

    @Override
    public Integer sortOrder()
    {
        return Integer.MAX_VALUE - 200;
    }
}



