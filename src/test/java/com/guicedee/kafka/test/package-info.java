/**
 * Kafka test messaging package.
 * Connection and topic definitions are declared at the package level.
 */
@KafkaConnectionOptions(
        value = "test-connection",
        bootstrapServers = "localhost:9092",
        groupId = "test-group"
)
package com.guicedee.kafka.test;

import com.guicedee.kafka.KafkaConnectionOptions;

