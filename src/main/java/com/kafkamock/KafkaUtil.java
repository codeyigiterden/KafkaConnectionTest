package com.kafkamock;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class KafkaUtil {

    public static final String EVENT_NAME_PREFIX = "DATA-";
    public static final String EVENT_TOPIC = "footopic";
    public static final String NON_EXIST_TOPIC = "NON_EXIST_TOPIC";
    private static final String KAFKA_BROKER = "localhost:9092";
    private static final String CONSUMER_GROUP_ID =  "mock-kafka-consumer-1";
    private static final String EARLIEST =  "earliest";
    private static final String ALL =  "all";
    private static final int START_INDEX = 1;

    /**
     * Returns Kafka properties for Consumer
     * @return
     */
    public static Properties getKafkaConsumerProperties() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
        props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG,false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, EARLIEST);
        return props;
    }

    /**
     * Returns Kafka properties for Producer
     * @return
     */
    public static Properties getKafkaProducerProperties() {
        final Properties props =  new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
        props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG,false);
        props.put(ProducerConfig.ACKS_CONFIG, ALL);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }

    /**
     * This function creates new topic with param name if not exists
     * @param topic
     */
    public static void createTopicIfNotExists(final String topic) {
        final NewTopic newTopic = new NewTopic(topic, Optional.empty(), Optional.empty());
        try (final AdminClient adminClient = getAdminClient(getKafkaProducerProperties())) {
            adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
        } catch (final InterruptedException | ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * This function creates Admin client
     * @param properties
     * @return AdminClient
     */
    public static AdminClient getAdminClient(Properties properties) {
        return AdminClient.create(properties);
    }

    /**
     * This function deletes all topics
     * @return AdminClient
     */
    public static void deleteAllTopics() {
        try (final AdminClient adminClient = getAdminClient(getKafkaProducerProperties())) {
            adminClient.deleteTopics(adminClient.listTopics().names().get());
        } catch (Exception e) {
           e.printStackTrace();
        }
    }

    /**
     * This function generates random messages
     * @param numberOfEvent
     * @return list of random message strings
     */
    public static List<String> generateRandomMessageList(int numberOfEvent) {
        return IntStream.rangeClosed(START_INDEX, numberOfEvent).mapToObj(i->generateRandomMessage(i)).collect(Collectors.toList());
    }


    /**
     * This function generates random message
     * @param seed
     * @return random message string
     */
    public static String generateRandomMessage(int seed) {
        return KafkaUtil.EVENT_NAME_PREFIX.concat(String.valueOf(seed).concat("#").concat(UUID.randomUUID().toString()));
    }
}
