package consumer;

import datamodel.CarNotificationData;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;
import java.util.Arrays;
import java.util.Properties;

@AllArgsConstructor
public class CarNotificationConsumer {
    private static final Logger LOGGER = Logger.getLogger(CarNotificationConsumer.class);
    private Properties properties;

    public void consumeNotificationEvent() {
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", properties.getProperty("com.iot.app.kafka.brokerlist"));
        consumerProps.put("group.id", properties.getProperty("com.iot.app.kafka.consumer.groupid"));
        consumerProps.put("enable.auto.commit", "true");
        consumerProps.put("auto.offset.reset", "earliest");
        consumerProps.put("auto.commit.interval.ms", "1000");
        consumerProps.put("session.timeout.ms", "30000");
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "decoder.CarNotificationDataDeserializer");
        consumerProps.put("deserializer.class", "decoder.CarNotificationDataDecoder");

        Consumer consumer = new KafkaConsumer<String,
                CarNotificationData>(consumerProps);
        consumer.subscribe(Arrays.asList(properties.getProperty("com.iot.app.kafka.topic")));

        while (true) {
            ConsumerRecords<String, CarNotificationData> records = consumer.poll(10);
            for (ConsumerRecord<String, CarNotificationData> record : records)
                System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
        }
    }
}
