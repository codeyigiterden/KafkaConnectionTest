package com.kafkamock;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

public class ConsumerApplication {

    private final Consumer<String, String> consumer;
    private static final long ONE_MIN = 5000;
    private static final Duration DURATION = Duration.ofMillis(100);
    private volatile boolean keepConsuming = true;

    public ConsumerApplication() {
        final Consumer<String, String> consumer = new KafkaConsumer<>(KafkaUtil.getKafkaConsumerProperties());
        this.consumer = consumer;
    }

    /**
     * Consumes messages inside the topic - footopic
     * It tries to consume the data with duration One minute
     * @return List of consumed message
     */
    public List<String> consume() {
        long start = System.currentTimeMillis();
        List<String> consumedData = new ArrayList<>();
        keepConsuming = true;
        while (keepConsuming) {
            ConsumerRecords<String, String> records = consumer.poll(DURATION);
            for (ConsumerRecord<String, String> record : records) {
                String value = record.value();
                consumedData.add(value);
                System.out.printf("Consumed record value %s %n", value);
            }
            checkOneMinPassed(start);
        }
        return consumedData;
    }

    /**
     * Consumer subscribes to the parameter topic
     * @param topic
     */
    public void subscribe(String topic) {
        consumer.subscribe(Arrays.asList(topic));
    }

    /**
     * This function closes the kafka consumer
     */
    public void close() {
        consumer.close();
    }

    /**
     * This function Returns list of partitions
     * @param topic
     * @return List of PartitionInfo
     */
    public List<PartitionInfo> getPartitionInfoList(String topic) {
        return consumer.partitionsFor(topic);
    }

    /**
     * This function returns map of topics
     * @return  Map<String, List<PartitionInfo>>
     */
    public Map<String, List<PartitionInfo>> getMapOfTopics() {
        return consumer.listTopics(Duration.ofMillis(1000));
    }

    /**
     * This function return last record of the parameter topic and partition number
     * @param topic -> topic name for last record inside partition
     * @param topicPartitionNumber -> partition Number of the topic
     * @return
     */
    public String getLastRecord(String topic, int topicPartitionNumber) {
        TopicPartition topicPartition = new TopicPartition(topic, topicPartitionNumber);
        Set<TopicPartition> topicPartitionList = Collections.singleton(topicPartition);
        Map<TopicPartition, Long> offsets = consumer.endOffsets(topicPartitionList);
        Long endOffset = offsets.entrySet().iterator().next().getValue();
        consumer.assign(topicPartitionList);
        consumer.seek(topicPartition,endOffset-1);
        return consume().get(0);
    }

    /**
     * This function checks if one minute passed
     * @param startTime
     */
    private void checkOneMinPassed(long startTime) {
        if(System.currentTimeMillis()-startTime > ONE_MIN) {
            keepConsuming = false;
        }
    }
}
