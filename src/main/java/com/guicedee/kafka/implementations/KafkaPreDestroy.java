package com.guicedee.kafka.implementations;

import com.guicedee.client.services.lifecycle.IGuicePreDestroy;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.producer.KafkaProducer;
import lombok.extern.log4j.Log4j2;

/**
 * Cleans up Kafka producers and consumers on application shutdown.
 */
@Log4j2
public class KafkaPreDestroy implements IGuicePreDestroy<KafkaPreDestroy>
{
    @Override
    public void onDestroy()
    {
        log.info("Shutting down Kafka consumers and producers...");

        // Close all topic-specific consumers
        KafkaPostStartup.getTopicConsumers().forEach((topicName, consumer) -> {
            try
            {
                consumer.close()
                        .onSuccess(v -> log.debug("Kafka consumer for topic '{}' closed.", topicName))
                        .onFailure(t -> log.error("Failed to close Kafka consumer for topic '{}'", topicName, t));
            }
            catch (Exception e)
            {
                log.error("Error closing Kafka consumer for topic '{}'", topicName, e);
            }
        });

        // Close all package-level consumers
        KafkaModule.getPackageConsumers().forEach((packageName, consumer) -> {
            try
            {
                consumer.close()
                        .onSuccess(v -> log.debug("Kafka consumer for package '{}' closed.", packageName))
                        .onFailure(t -> log.error("Failed to close Kafka consumer for package '{}'", packageName, t));
            }
            catch (Exception e)
            {
                log.error("Error closing Kafka consumer for package '{}'", packageName, e);
            }
        });

        // Close all package-level producers
        KafkaModule.getPackageProducers().forEach((packageName, producer) -> {
            try
            {
                producer.close()
                        .onSuccess(v -> log.debug("Kafka producer for package '{}' closed.", packageName))
                        .onFailure(t -> log.error("Failed to close Kafka producer for package '{}'", packageName, t));
            }
            catch (Exception e)
            {
                log.error("Error closing Kafka producer for package '{}'", packageName, e);
            }
        });

        // Close all admin clients
        KafkaModule.getAdminClients().forEach((connectionName, adminClient) -> {
            try
            {
                adminClient.close()
                        .onSuccess(v -> log.debug("Kafka admin client for connection '{}' closed.", connectionName))
                        .onFailure(t -> log.error("Failed to close Kafka admin client for connection '{}'", connectionName, t));
            }
            catch (Exception e)
            {
                log.error("Error closing Kafka admin client for connection '{}'", connectionName, e);
            }
        });

        // Close any admin clients created during topic creation
        KafkaPostStartup.getAdminClients().forEach((connectionName, adminClient) -> {
            try
            {
                adminClient.close()
                        .onSuccess(v -> log.debug("Kafka post-startup admin client for '{}' closed.", connectionName))
                        .onFailure(t -> log.error("Failed to close post-startup admin client for '{}'", connectionName, t));
            }
            catch (Exception e)
            {
                log.error("Error closing post-startup admin client for '{}'", connectionName, e);
            }
        });

        log.info("Kafka shutdown complete.");
    }

    @Override
    public Integer sortOrder()
    {
        return Integer.MAX_VALUE - 100;
    }
}


