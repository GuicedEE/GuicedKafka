package com.guicedee.kafka.implementations;

import com.guicedee.client.IGuiceContext;
import com.guicedee.client.services.lifecycle.IGuicePreStartup;
import com.guicedee.kafka.*;
import com.guicedee.vertx.spi.VertXPreStartup;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;
import io.vertx.core.Future;
import com.google.inject.Key;
import com.google.inject.name.Names;
import jakarta.inject.Named;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Pre-startup scanner that discovers Kafka annotations and registers
 * topics, consumers, and publishers for later binding.
 */
@Log4j2
public class KafkaPreStartup implements IGuicePreStartup<KafkaPreStartup>
{
    /**
     * Package-level connection options.
     */
    @Getter
    private static final Map<String, List<KafkaConnectionOptions>> packageKafkaConnections = new TreeMap<>();

    /**
     * Topic consumer definitions keyed by topic name.
     */
    @Getter
    private static final Map<String, KafkaTopicDefinition> topicConsumerDefinitions = new HashMap<>();

    /**
     * Consumer classes keyed by topic name.
     */
    @Getter
    private static final Map<String, Class<? extends KafkaTopicConsumer>> topicConsumerClass = new HashMap<>();

    /**
     * Consumer Guice keys keyed by topic name.
     */
    @Getter
    private static final Map<String, Key<?>> topicConsumerKeys = new HashMap<>();

    /**
     * Topic publisher definitions keyed by topic name.
     */
    @Getter
    private static final Map<String, KafkaTopicDefinition> topicPublisherDefinitions = new HashMap<>();

    /**
     * Publisher Guice keys keyed by topic name.
     */
    @Getter
    private static final Map<String, Key<?>> topicPublisherKeys = new HashMap<>();

    /**
     * Maps topic name to connection name.
     */
    @Getter
    private static final Map<String, String> topicConnectionNames = new HashMap<>();

    /**
     * Topic creation definitions discovered from @KafkaTopicCreate annotations.
     */
    @Getter
    private static final Map<String, KafkaTopicCreate> topicCreateDefinitions = new HashMap<>();

    /**
     * Maps package name to connection name for package-level connections.
     */
    @Getter
    private static final Map<String, String> packageConnectionNames = new HashMap<>();

    @Override
    public List<Future<Boolean>> onStartup()
    {
        return List.of(VertXPreStartup.getVertx().executeBlocking(() -> {
            ScanResult scanResult = IGuiceContext.instance().getScanResult();
            Set<Class<?>> completedConsumers = new HashSet<>();
            processConnections(scanResult, completedConsumers);
            processPackageConnections(scanResult, completedConsumers);
            return true;
        }));
    }

    private void processConnections(ScanResult scanResult, Set<Class<?>> completedConsumers)
    {
        ClassInfoList connectionClasses = scanResult.getClassesWithAnnotation(KafkaConnectionOptions.class);
        connectionClasses.stream()
                .distinct()
                .forEach(ci -> processConnection(scanResult, ci, completedConsumers, false));
        connectionClasses.stream()
                .distinct()
                .forEach(ci -> processConnection(scanResult, ci, completedConsumers, true));
    }

    /**
     * Scans for @KafkaConnectionOptions on package-info.java files.
     */
    private void processPackageConnections(ScanResult scanResult, Set<Class<?>> completedConsumers)
    {
        var packageInfoList = scanResult.getPackageInfo();
        if (packageInfoList != null)
        {
            packageInfoList.forEach(packageInfo -> {
                try
                {
                    var packageName = packageInfo.getName();
                    // Try to load the package-info class
                    String packageInfoClassName = packageName + ".package-info";
                    try
                    {
                        Class<?> packageInfoClass = Class.forName(packageInfoClassName);
                        processPackageInfoClass(scanResult, packageInfoClass, packageName, completedConsumers);
                    }
                    catch (ClassNotFoundException ignored)
                    {
                        // No package-info.java for this package — that's fine
                    }
                }
                catch (Exception e)
                {
                    log.trace("Error processing package info: {}", e.getMessage());
                }
            });
        }
    }

    private void processPackageInfoClass(ScanResult scanResult, Class<?> packageInfoClass, String packageName, Set<Class<?>> completedConsumers)
    {
        var connectionAnnotation = packageInfoClass.getAnnotation(KafkaConnectionOptions.class);
        if (connectionAnnotation != null && !packageKafkaConnections.containsKey(packageName))
        {
            String connectionName = connectionAnnotation.value();
            KafkaConnectionOptions wrapped = wrapConnectionOptions(connectionName, connectionAnnotation);
            packageKafkaConnections.computeIfAbsent(packageName, k -> new ArrayList<>()).add(wrapped);
            packageConnectionNames.put(packageName, connectionName);

            // Scan for topic creation annotations
            processTopicCreateAnnotations(packageInfoClass, connectionName);

            // Scan package classes for consumers and publishers
            var packageClassInfos = scanResult.getPackageInfo(packageName);
            if (packageClassInfos != null)
            {
                var classInfos = packageClassInfos.getClassInfoRecursive();
                processConsumers(classInfos, connectionName, completedConsumers);
                processPublishers(classInfos, connectionName);
            }
        }

        // Also check for topic creation annotations on this class
        processTopicCreateAnnotations(packageInfoClass, packageConnectionNames.getOrDefault(packageName, "default"));
    }

    /**
     * Processes @KafkaTopicCreate and @KafkaTopicCreates annotations.
     */
    private void processTopicCreateAnnotations(Class<?> clazz, String connectionName)
    {
        // Handle repeatable annotations
        KafkaTopicCreate[] creates = clazz.getAnnotationsByType(KafkaTopicCreate.class);
        if (creates != null)
        {
            for (KafkaTopicCreate create : creates)
            {
                String topicName = create.value();
                topicCreateDefinitions.putIfAbsent(topicName, create);
                topicConnectionNames.putIfAbsent(topicName, connectionName);
                log.debug("Found Kafka Topic Create - {} (partitions={}, replication={})", topicName, create.partitions(), create.replicationFactor());
            }
        }
    }

    private void processConnection(ScanResult scanResult, ClassInfo classInfo, Set<Class<?>> completedConsumers, boolean publishers)
    {
        log.debug("Found Kafka Connection - {}", classInfo.getName());

        var connectionAnnotation = classInfo.loadClass().getAnnotation(KafkaConnectionOptions.class);
        String connectionName = connectionAnnotation.value();
        KafkaConnectionOptions wrapped = wrapConnectionOptions(connectionName, connectionAnnotation);
        packageKafkaConnections.computeIfAbsent(classInfo.getPackageName(), k -> new ArrayList<>()).add(wrapped);
        packageConnectionNames.put(classInfo.getPackageName(), connectionName);

        // Scan for topic creation annotations on this class too
        processTopicCreateAnnotations(classInfo.loadClass(), connectionName);

        var classInfos = scanResult.getPackageInfo(classInfo.getPackageName()).getClassInfoRecursive();

        if (!publishers)
        {
            processConsumers(classInfos, connectionName, completedConsumers);
        }
        else
        {
            processPublishers(classInfos, connectionName);
        }
    }

    @SuppressWarnings("unchecked")
    private void processConsumers(List<ClassInfo> classInfos, String connectionName, Set<Class<?>> completedConsumers)
    {
        var consumers = classInfos.stream()
                .filter(info -> info.hasAnnotation(KafkaTopicDefinition.class))
                .distinct()
                .toList();

        for (ClassInfo consumerClassInfo : consumers)
        {
            Class<? extends KafkaTopicConsumer> consumerClass = (Class<? extends KafkaTopicConsumer>) consumerClassInfo.loadClass();
            if (completedConsumers.contains(consumerClass))
            {
                continue;
            }
            completedConsumers.add(consumerClass);

            var topicDef = consumerClass.getAnnotation(KafkaTopicDefinition.class);
            String topicName = topicDef.value();
            KafkaTopicDefinition wrappedDef = wrapTopicDefinition(topicDef);

            topicConsumerDefinitions.put(topicName, wrappedDef);
            topicConsumerClass.put(topicName, consumerClass);
            topicConsumerKeys.put(topicName, Key.get(consumerClass, Names.named(topicName)));
            topicConnectionNames.putIfAbsent(topicName, connectionName);

            // Also register publisher for this topic
            registerPublisher(topicName, connectionName, wrappedDef);

            log.debug("Found Kafka Topic Consumer - {} - {}", topicName, connectionName);
        }
    }

    private void processPublishers(List<ClassInfo> classInfos, String connectionName)
    {
        classInfos.stream()
                .filter(info -> info.hasDeclaredFieldAnnotation(KafkaTopicDefinition.class) ||
                        info.getFieldInfo().stream()
                                .anyMatch(a -> a.getTypeSignatureOrTypeDescriptor().toString().equals("com.guicedee.kafka.KafkaTopicPublisher")))
                .forEach(publisherClassInfo -> registerFieldPublishers(publisherClassInfo, connectionName));
    }

    private void registerFieldPublishers(ClassInfo publisherClassInfo, String connectionName)
    {
        var fields = getPublisherFields(publisherClassInfo);
        for (Field field : fields)
        {
            String topicName = getTopicNameFromField(field, publisherClassInfo);
            if (topicName != null && !topicPublisherDefinitions.containsKey(topicName))
            {
                var topicDef = field.getAnnotation(KafkaTopicDefinition.class);
                KafkaTopicDefinition wrappedDef = topicDef != null ? wrapTopicDefinition(topicDef) : createDefaultTopicDefinition(topicName);
                registerPublisher(topicName, connectionName, wrappedDef);
                log.debug("Found Kafka Topic Publisher - {} - {}", topicName, connectionName);
            }
        }
    }

    private void registerPublisher(String topicName, String connectionName, KafkaTopicDefinition wrappedDef)
    {
        topicPublisherDefinitions.putIfAbsent(topicName, wrappedDef);
        topicPublisherKeys.putIfAbsent(topicName, Key.get(KafkaTopicPublisher.class, Names.named(topicName)));
        topicConnectionNames.putIfAbsent(topicName, connectionName);
    }

    private String getTopicNameFromField(Field field, ClassInfo publisherClassInfo)
    {
        if (field.isAnnotationPresent(Named.class))
        {
            return field.getAnnotation(Named.class).value();
        }
        else if (field.isAnnotationPresent(com.google.inject.name.Named.class))
        {
            return field.getAnnotation(com.google.inject.name.Named.class).value();
        }
        else
        {
            var topicDef = publisherClassInfo.loadClass().getAnnotation(KafkaTopicDefinition.class);
            return topicDef != null ? topicDef.value() : null;
        }
    }

    private List<Field> getPublisherFields(ClassInfo aClass)
    {
        Class<?> clazz = aClass.loadClass();
        return Arrays.stream(clazz.getDeclaredFields())
                .filter(field -> !Modifier.isFinal(field.getModifiers()) && !Modifier.isStatic(field.getModifiers()))
                .filter(field -> {
                    boolean hasInject = field.isAnnotationPresent(com.google.inject.Inject.class) || field.isAnnotationPresent(jakarta.inject.Inject.class);
                    boolean hasNamed = field.isAnnotationPresent(Named.class) || field.isAnnotationPresent(com.google.inject.name.Named.class);
                    return hasInject && hasNamed && field.getType().equals(KafkaTopicPublisher.class);
                })
                .collect(Collectors.toList());
    }

    private KafkaTopicDefinition createDefaultTopicDefinition(String topicName)
    {
        return new KafkaTopicDefinition()
        {
            @Override
            public Class<? extends Annotation> annotationType()
            {
                return KafkaTopicDefinition.class;
            }

            @Override
            public String value()
            {
                return envForName(topicName, "TOPIC_NAME", topicName);
            }

            @Override
            public KafkaTopicOptions options()
            {
                return new KafkaTopicOptionsDefault();
            }
        };
    }

    private KafkaTopicDefinition wrapTopicDefinition(KafkaTopicDefinition def)
    {
        if (def == null) return null;
        String rawName = def.value();
        return new KafkaTopicDefinition()
        {
            @Override
            public Class<? extends Annotation> annotationType()
            {
                return KafkaTopicDefinition.class;
            }

            @Override
            public String value()
            {
                return envForName(rawName, "TOPIC_NAME", def.value());
            }

            @Override
            public KafkaTopicOptions options()
            {
                return wrapTopicOptions(rawName, def.options());
            }
        };
    }

    private KafkaTopicOptions wrapTopicOptions(String topicName, KafkaTopicOptions options)
    {
        if (options == null) return null;
        return new KafkaTopicOptions()
        {
            @Override
            public Class<? extends Annotation> annotationType()
            {
                return KafkaTopicOptions.class;
            }

            @Override
            public boolean autoCommit()
            {
                return Boolean.parseBoolean(envForName(topicName, "TOPIC_AUTO_COMMIT", String.valueOf(options.autoCommit())));
            }

            @Override
            public boolean worker()
            {
                return Boolean.parseBoolean(envForName(topicName, "TOPIC_WORKER", String.valueOf(options.worker())));
            }

            @Override
            public int consumerCount()
            {
                return Integer.parseInt(envForName(topicName, "TOPIC_CONSUMER_COUNT", String.valueOf(options.consumerCount())));
            }

            @Override
            public int partition()
            {
                return Integer.parseInt(envForName(topicName, "TOPIC_PARTITION", String.valueOf(options.partition())));
            }

            @Override
            public int maxPollIntervalMs()
            {
                return Integer.parseInt(envForName(topicName, "TOPIC_MAX_POLL_INTERVAL_MS", String.valueOf(options.maxPollIntervalMs())));
            }

            @Override
            public boolean pauseOnStart()
            {
                return Boolean.parseBoolean(envForName(topicName, "TOPIC_PAUSE_ON_START", String.valueOf(options.pauseOnStart())));
            }
        };
    }

    @SuppressWarnings("unchecked")
    private KafkaConnectionOptions wrapConnectionOptions(String connectionName, KafkaConnectionOptions ann)
    {
        return new KafkaConnectionOptions()
        {
            @Override
            public Class<? extends Annotation> annotationType() { return KafkaConnectionOptions.class; }

            @Override
            public String value() { return envForName(connectionName, "CONNECTION_NAME", ann.value()); }

            @Override
            public String bootstrapServers() { return envForName(connectionName, "BOOTSTRAP_SERVERS", ann.bootstrapServers()); }

            @Override
            public String groupId() { return envForName(connectionName, "GROUP_ID", ann.groupId()); }

            @Override
            public String keyDeserializer() { return envForName(connectionName, "KEY_DESERIALIZER", ann.keyDeserializer()); }

            @Override
            public String valueDeserializer() { return envForName(connectionName, "VALUE_DESERIALIZER", ann.valueDeserializer()); }

            @Override
            public String keySerializer() { return envForName(connectionName, "KEY_SERIALIZER", ann.keySerializer()); }

            @Override
            public String valueSerializer() { return envForName(connectionName, "VALUE_SERIALIZER", ann.valueSerializer()); }

            @Override
            public String autoOffsetReset() { return envForName(connectionName, "AUTO_OFFSET_RESET", ann.autoOffsetReset()); }

            @Override
            public boolean enableAutoCommit() { return Boolean.parseBoolean(envForName(connectionName, "ENABLE_AUTO_COMMIT", String.valueOf(ann.enableAutoCommit()))); }

            @Override
            public int autoCommitIntervalMs() { return Integer.parseInt(envForName(connectionName, "AUTO_COMMIT_INTERVAL_MS", String.valueOf(ann.autoCommitIntervalMs()))); }

            @Override
            public String acks() { return envForName(connectionName, "ACKS", ann.acks()); }

            @Override
            public int retries() { return Integer.parseInt(envForName(connectionName, "RETRIES", String.valueOf(ann.retries()))); }

            @Override
            public int lingerMs() { return Integer.parseInt(envForName(connectionName, "LINGER_MS", String.valueOf(ann.lingerMs()))); }

            @Override
            public int batchSize() { return Integer.parseInt(envForName(connectionName, "BATCH_SIZE", String.valueOf(ann.batchSize()))); }

            @Override
            public long bufferMemory() { return Long.parseLong(envForName(connectionName, "BUFFER_MEMORY", String.valueOf(ann.bufferMemory()))); }

            @Override
            public int requestTimeoutMs() { return Integer.parseInt(envForName(connectionName, "REQUEST_TIMEOUT_MS", String.valueOf(ann.requestTimeoutMs()))); }

            @Override
            public int sessionTimeoutMs() { return Integer.parseInt(envForName(connectionName, "SESSION_TIMEOUT_MS", String.valueOf(ann.sessionTimeoutMs()))); }

            @Override
            public int maxPollRecords() { return Integer.parseInt(envForName(connectionName, "MAX_POLL_RECORDS", String.valueOf(ann.maxPollRecords()))); }

            @Override
            public String clientId() { return envForName(connectionName, "CLIENT_ID", ann.clientId()); }
        };
    }

    @Override
    public Integer sortOrder()
    {
        return Integer.MIN_VALUE + 80;
    }

    /**
     * Resolves an environment variable or system property scoped by name.
     * Lookup order:
     * 1. KAFKA_{NORMALIZED_NAME}_{PROPERTY}
     * 2. KAFKA_{PROPERTY}
     * 3. defaultValue
     */
    static String envForName(String name, String property, String defaultValue)
    {
        String normalizedName = name.toUpperCase().replace('-', '_').replace('.', '_');
        String scopedKey = "KAFKA_" + normalizedName + "_" + property;
        String scopedValue = com.guicedee.client.Environment.getSystemPropertyOrEnvironment(scopedKey, null);
        if (scopedValue != null && !scopedValue.isBlank())
        {
            return scopedValue;
        }
        return com.guicedee.client.Environment.getSystemPropertyOrEnvironment("KAFKA_" + property, defaultValue);
    }
}

