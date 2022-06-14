package com.kafkamock;

import com.github.dockerjava.api.model.Container;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.Test;
import java.util.List;
import static org.junit.Assert.*;

/**
 * Main purpose of this class to tests Apache Kafka Connection
 */
public class KafkaTest {

    private static final int NUMBER_OF_TWENTY_THOUSAND= 20000;
    private static final int NUMBER_OF_TEN = 10;
    private static final int NUMBER_OF_FIVE= 5;
    private static final int NUMBER_OF_ZERO= 0;
    /**
     * This test checks if consumer application successfully connects the broker using
     * checking map of the topics
     */
    @Test
    public void kafkaConnectionBrokerSuccessfulTest(){
        try {
            ConsumerApplication consumerApplication = new ConsumerApplication();
            consumerApplication.getMapOfTopics();
            assertTrue("Consumer can connect broker!",true);
        }catch (TimeoutException timeoutException) {
            fail("Timeout occured, cannot connect to Broker");
        }
        catch (Exception e) {
            fail("Exception occured");
        }
    }

    /**
     * This test checks if consumer application not connects the broker
     */
    @Test(expected = TimeoutException.class)
    public void kafkaConnectionBrokerFailTest(){
        ConsumerApplication consumerApplication = new ConsumerApplication();
        consumerApplication.getMapOfTopics();
    }

    /**
     * This test checks if consumer connects to the topic via partition info list
     */
    @Test
    public void kafkaConnectionTopicSuccessfulTest(){
        ConsumerApplication consumerApplication = new ConsumerApplication();
        List<PartitionInfo> partitionInfoList = consumerApplication.getPartitionInfoList(KafkaUtil.EVENT_TOPIC);
        assertTrue("Topic doesnt have partition",partitionInfoList.size() > NUMBER_OF_ZERO);
    }

    /**
     * This test checks if consumer cannot connects to the topic via partition info list
     */
    @Test
    public void kafkaConnectionTopicFailTest(){
        ConsumerApplication consumerApplication = new ConsumerApplication();
        List<PartitionInfo> partitionInfoList = consumerApplication.getPartitionInfoList(KafkaUtil.NON_EXIST_TOPIC);
        assertTrue("Cannot find topic in broker",partitionInfoList.size() <= NUMBER_OF_ZERO);
    }

    /**
     * This test checks all records of the topic's partition consumed correctly
     */
    @Test
    public void kafkaConsumeAllRecordsSuccessfulTest(){
        ProducerApplication producerApplication = new ProducerApplication(KafkaUtil.EVENT_TOPIC);
        List<String> messageList = KafkaUtil.generateRandomMessageList(NUMBER_OF_TEN);
        messageList.forEach(message -> producerApplication.produce(message));
        producerApplication.flush();
        System.out.println(messageList.size() + " messages were produced to topic " +KafkaUtil.EVENT_TOPIC);
        producerApplication.close();

        ConsumerApplication consumerApplication = new ConsumerApplication();
        consumerApplication.subscribe(KafkaUtil.EVENT_TOPIC);
        List<String> consumedRecords = consumerApplication.consume();
        System.out.println("ConsumedRecords:"+consumedRecords);
        consumerApplication.close();
        assertTrue("Produced List is not match with Consumed List",messageList.equals(consumedRecords));
    }

    /**
     * This test checks latest record of the topic's partition consumed correctly
     */
    @Test
    public void kafkaConsumeLastRecordSuccessfulTest(){
        ProducerApplication producerApplication = new ProducerApplication(KafkaUtil.EVENT_TOPIC);
        List<String> messageList = KafkaUtil.generateRandomMessageList(NUMBER_OF_FIVE);
        messageList.forEach(message -> producerApplication.produce(message));
        producerApplication.flush();
        System.out.println(messageList.size() + " messages were produced to topic " + KafkaUtil.EVENT_TOPIC);
        producerApplication.close();

        ConsumerApplication consumerApplication = new ConsumerApplication();
        String lastRecord = consumerApplication.getLastRecord(KafkaUtil.EVENT_TOPIC,NUMBER_OF_ZERO);
        System.out.println("LastRecord: "+lastRecord);
        consumerApplication.close();
        assertTrue("Message is not match with latest Data",messageList.get(4).equals(lastRecord));
    }

    /**
     * This test check messaged consumed correctly first. Then program sends new messages to kafka,
     * but before program consumes, it shutdowns kafka,
     * then tries to consume the messagges
     */
    @Test
    public void kafkaShutdownTest(){
        //FIRST STEP -> Producer produces new messages while kafka is running
        ProducerApplication producerApplication = new ProducerApplication(KafkaUtil.EVENT_TOPIC);
        ConsumerApplication consumerApplication = new ConsumerApplication();
        consumerApplication.subscribe(KafkaUtil.EVENT_TOPIC);

        //SECOND STEP -> Producer produces new random messages while kafka is running
        System.out.println("FIRST STEP Started -> producing new set of messages");
        List<String> secondStepMessageList = KafkaUtil.generateRandomMessageList(NUMBER_OF_FIVE);
        secondStepMessageList.forEach(message -> producerApplication.produce(message));
        producerApplication.flush();
        producerApplication.close();
        System.out.println("FIRST STEP -> " + secondStepMessageList.size() + " messages were produced to topic " + KafkaUtil.EVENT_TOPIC);

        //SECOND STEP -> Shutdown the kafka
        System.out.println("Shutting Down Kafka!");
        DockerLocalClient dockerLocalClient = new DockerLocalClient();
        List<Container> containerList =  dockerLocalClient.getContainerList();
        containerList.forEach(container -> dockerLocalClient.stopContainer(container.getId()));
        System.out.println("Kafka is offline!");

        sleep(NUMBER_OF_TWENTY_THOUSAND);

        //SECOND STEP -> Consumer Tries to consume while kafka is offline
        List<String> secondStepConsumedRecords = consumerApplication.consume();
        System.out.println("SECOND STEP -> " + secondStepConsumedRecords.size() + " messages were consumed from topic " + KafkaUtil.EVENT_TOPIC);
        assertTrue("Consumed List is not empty", secondStepConsumedRecords.size() <= NUMBER_OF_ZERO);

        //THIRD STEP -> Start kafka again and try to consume the data
        System.out.println("Starting Kafka!");
        containerList.forEach(container -> dockerLocalClient.startContainer(container.getId()));
        System.out.println("Kafka is online!");

        sleep(NUMBER_OF_TWENTY_THOUSAND);

        List<String> thirdStepConsumedRecords = consumerApplication.consume();
        System.out.println("THIRD STEP -> " + thirdStepConsumedRecords.size() + " messages were consumed from topic " + KafkaUtil.EVENT_TOPIC);
        assertTrue("Produced List is not match with Consumed List After Kafka Started", secondStepMessageList.equals(thirdStepConsumedRecords));


        consumerApplication.close();
    }

    private void sleep(int milliseconds) {
        try {
            Thread.sleep(milliseconds);
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
}
