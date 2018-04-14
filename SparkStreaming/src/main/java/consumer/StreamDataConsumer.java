package consumer;

import datamodel.CarData;
import datamodel.CarNotificationData;
import decoder.CarDataDecoder;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringDecoder;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import utils.reader.PropertyFileReader;
import java.util.*;

public class StreamDataConsumer {
    private static final Logger LOGGER = Logger.getLogger(StreamDataConsumer.class);

    public void consume() {
        Properties properties = PropertyFileReader.readConsumerPropertyFile();
        Properties prodProperties = PropertyFileReader.readProducerPropertyFile();
        SparkConf sparkConf = new SparkConf()
                .setAppName(properties.getProperty("com.iot.app.spark.app.name"))
                .setMaster(properties.getProperty("com.iot.app.spark.master"));

        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));
        javaStreamingContext.checkpoint(properties.getProperty("com.iot.app.spark.checkpoint.dir"));

        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("zookeeper.connect", properties.getProperty("com.iot.app.kafka.zookeeper"));
        kafkaParams.put("metadata.broker.list", properties.getProperty("com.iot.app.kafka.brokerlist"));

        String topic = properties.getProperty("com.iot.app.kafka.topic");
        Set<String> topicSet = new HashSet<String>();
        topicSet.add(topic);

        JavaPairInputDStream<String, CarData> directKafkaStream = KafkaUtils.createDirectStream(
                javaStreamingContext,
                String.class,
                CarData.class,
                StringDecoder.class,
                CarDataDecoder.class,
                kafkaParams,
                topicSet
        );

        LOGGER.info("Started stream processing...");
        JavaDStream<CarData> nonFilteredDataStream = directKafkaStream.map(tuple -> tuple._2());
        produceCarNotifications(nonFilteredDataStream, prodProperties);
        //nonFilteredDataStream.print();

        javaStreamingContext.start();
        try {
            javaStreamingContext.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void produceCarNotifications(JavaDStream<CarData> dStream, Properties properties) {
        dStream.foreachRDD(
                rdd -> {
                    for (CarData carData : rdd.collect()) {
                        Properties producerProperties = new Properties();
                        producerProperties.put("zookeeper.connect",
                                properties.getProperty("com.iot.app.kafka.zookeeper"));
                        producerProperties.put("metadata.broker.list",
                                properties.getProperty("com.iot.app.kafka.brokerlist"));
                        producerProperties.put("request.required.acks", "1");
                        producerProperties.put("serializer.class", "encoder.CarNotificationDataEncoder");

                        Producer<String, CarNotificationData> producer = new Producer<>
                                (new ProducerConfig(producerProperties));
                        String topic = properties.getProperty("com.iot.app.kafka.topic");
                        Random random = new Random();

                        CarNotificationData carNotificationData = new CarNotificationData(carData.getCarId(),
                                carData.getSpeed());

                        KeyedMessage<String, CarNotificationData> keyedMessage = new KeyedMessage<>(topic,
                                carNotificationData);
                        producer.send(keyedMessage);
                    }
                }
        );
    }
}
