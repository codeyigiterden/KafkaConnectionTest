package com.kafkamock;

import org.apache.kafka.clients.producer.*;
import java.util.Properties;

public class ProducerApplication {

    private final Producer<String, String> producer;
    private final String topic;

    public ProducerApplication(final String topic) {
        KafkaUtil.createTopicIfNotExists(KafkaUtil.EVENT_TOPIC);
        final Properties props =  KafkaUtil.getKafkaProducerProperties();
        Producer<String, String> producer = new KafkaProducer<>(props);

        this.producer = producer;
        this.topic = topic;
    }

    /**
     * This function produces sends new message to the kafka
     * @param message
     */
    public void produce(String message) {
        ProducerRecord producerRecord = new ProducerRecord<>(topic, message);
        producer.send(producerRecord, (recordMetadata, exception) -> {
            if (exception == null) {
                System.out.println("Record written to offset " +
                        recordMetadata.offset() + " Message " +
                        producerRecord.value());
            } else {
                System.err.println("An error occurred");
                exception.printStackTrace(System.err);
            }
        });
    }

    /**
     * This function flushes producer
     */
    public void flush() {
        producer.flush();
    }

    /**
     * This function closes producer
     */
    public void close() {
        producer.close();
    }
}
