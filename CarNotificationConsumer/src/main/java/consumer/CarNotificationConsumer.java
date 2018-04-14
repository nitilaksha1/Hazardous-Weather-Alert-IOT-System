package consumer;

import datamodel.CarNotificationData;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;
import scala.collection.immutable.Stream;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

@AllArgsConstructor
public class CarNotificationConsumer {
    private static final Logger LOGGER = Logger.getLogger(CarNotificationConsumer.class);
    private Properties properties;

    public void consumeNotificationEvent() {
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", properties.getProperty("com.iot.app.kafka.brokerlist"));
        consumerProps.put("group.id", properties.getProperty("com.iot.app.kafka.consumer.groupid"));
        consumerProps.put("enable.auto.commit", "true");
        consumerProps.put("auto.commit.interval.ms", "1000");
        consumerProps.put("session.timeout.ms", "30000");
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "decoder.CarNotificationDataDeserializer");
        consumerProps.put("deserializer.class", "decoder.CarNotificationDataDecoder");
        consumerProps.put("auto.offset.reset", "earliest");

        Consumer consumer = new KafkaConsumer<String,
                CarNotificationData>(consumerProps);
        consumer.subscribe(Arrays.asList(properties.getProperty("com.iot.app.kafka.topic")));
        //Set<TopicPartition> assignedTopicPartitions = consumer.assignment();

//        for (TopicPartition assignedPartition: assignedTopicPartitions) {
//            consumer.seekToBeginning(assignedPartition);
//        }

        //consumer.seekToBeginning();

        while (true) {
            ConsumerRecords records = consumer.poll(0);
            for (Object record : records) {
                ConsumerRecord cr = (ConsumerRecord) record;
                CarNotificationData carNotificationData = (CarNotificationData) cr.value();

                System.out.println("Key = " + cr.key() + " CarId = " + carNotificationData.getCarId() +
                " Car Speed = " + carNotificationData.getSpeed());
            }
                //System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
        }
    }
}
