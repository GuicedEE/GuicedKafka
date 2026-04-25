# Guiced Kafka

Annotation-driven Apache Kafka integration for GuicedEE using the Vert.x Kafka client.

## Core Concept

Declare connections, topics, consumers, and publishers with annotations — everything is discovered at startup via ClassGraph, wired through Guice, and managed by the Vert.x Kafka client.

## Quick Start

### 1. Add dependency

```xml
<dependency>
    <groupId>com.guicedee</groupId>
    <artifactId>kafka</artifactId>
    <version>2.0.0-RC9</version>
</dependency>
```

### 2. Define a connection and consumer

```java
@KafkaConnectionOptions(
    value = "my-connection",
    bootstrapServers = "localhost:9092",
    groupId = "my-group"
)
@KafkaTopicDefinition(
    value = "order-events",
    options = @KafkaTopicOptions(worker = true)
)
public class OrderConsumer implements KafkaTopicConsumer<String, String> {
    @Override
    public void consume(KafkaConsumerRecord<String, String> record) {
        System.out.println("Received: " + record.value());
    }
}
```

### 3. Inject a publisher

```java
public class OrderService {
    @Inject @Named("order-events")
    private KafkaTopicPublisher orderPublisher;

    public void placeOrder(String orderJson) {
        orderPublisher.send("order-key", orderJson);
    }
}
```

### 4. Configure `module-info.java`

```java
module my.app {
    requires com.guicedee.kafka;
    opens my.app.messaging to com.google.guice, com.guicedee.kafka;
}
```

## Annotations

### `@KafkaConnectionOptions`
Connection configuration: bootstrap servers, group id, serializers/deserializers, acks, retries, timeouts.

### `@KafkaTopicDefinition`
Topic declaration: name, options for consumer tuning.

### `@KafkaTopicOptions`
Consumer tuning: auto-commit, worker threads, consumer count, partition assignment, pause on start.

## Environment Variable Overrides

Every annotation attribute can be overridden via system properties or environment variables:
- `KAFKA_{NORMALIZED_NAME}_{PROPERTY}` — name-specific override
- `KAFKA_{PROPERTY}` — global fallback

## Startup Flow

```
IGuiceContext.instance().inject()
 └─ KafkaPreStartup (annotation scanning)
     ├─ Discovers @KafkaConnectionOptions
     ├─ Discovers @KafkaTopicDefinition consumers
     └─ Registers metadata for binding
 └─ KafkaModule (Guice bindings)
     ├─ Creates KafkaProducer per connection
     ├─ Creates KafkaConsumer per connection
     ├─ Binds KafkaTopicConsumer as singletons
     └─ Binds KafkaTopicPublisher as @Named("topic-name")
 └─ KafkaPostStartup (runtime initialization)
     ├─ Creates per-topic consumers
     ├─ Subscribes to topics
     └─ Starts consuming with call-scoped message handling
```

