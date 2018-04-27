package simulator;

import datamodel.Precipitation;
import datamodel.WeatherData;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import lombok.AllArgsConstructor;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

@AllArgsConstructor
public class NormalWeatherSimulator implements WeatherDataSimulator {
    private static final Logger LOGGER = Logger.getLogger(NormalWeatherSimulator.class);
    private Properties properties;
    private double latitude;
    private double longitude;

    public void generateWeatherEvent() {
        Producer<String, WeatherData> producer = createProducer();

        String topic = properties.getProperty("com.iot.app.kafka.topic");
        Random random = new Random();

        LOGGER.info("Sending normal weather events ...");

        while(true) {
            WeatherData weatherData = new WeatherData(UUID.randomUUID().toString(),
                    latitude,
                    longitude,
                    new DateTime().getMillis(),
                    random.nextInt(5) + 22,
                    Precipitation.NORMAL,
                    random.nextInt(3) + 1,
                    random.nextInt(2) + 1);

            producer.send(new KeyedMessage<String, WeatherData>(topic, weatherData));

            try {
                Thread.sleep(random.nextInt(2000) + 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private Producer<String, WeatherData> createProducer() {
        Properties producerProperties = new Properties();

        producerProperties.put("zookeeper.connect", properties.getProperty("com.iot.app.kafka.zookeeper"));
        producerProperties.put("metadata.broker.list", properties.getProperty("com.iot.app.kafka.brokerlist"));
        producerProperties.put("request.required.acks", "1");
        producerProperties.put("serializer.class", "encoder.WeatherDataEncoder");

        return new Producer<String, WeatherData>(new ProducerConfig(producerProperties));
    }
}
