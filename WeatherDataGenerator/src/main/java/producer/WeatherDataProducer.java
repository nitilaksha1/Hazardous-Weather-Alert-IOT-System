package producer;

import datamodel.WeatherData;
import kafka.producer.KeyedMessage;
import lombok.AllArgsConstructor;
import org.apache.log4j.Logger;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import org.joda.time.DateTime;
import java.util.*;

@AllArgsConstructor
public class WeatherDataProducer {
    private static final Logger LOGGER = Logger.getLogger(WeatherDataProducer.class);
    private Properties properties;

    public void generateIotEvent(int numSensors, int numEvents) {
        Properties producerProperties = new Properties();
        producerProperties.put("zookeeper.connect", properties.getProperty("com.iot.app.kafka.zookeeper"));
        producerProperties.put("metadata.broker.list", properties.getProperty("com.iot.app.kafka.brokerlist"));
        producerProperties.put("request.required.acks", "1");
        producerProperties.put("serializer.class", "encoder.WeatherDataEncoder");

        Producer<String, WeatherData> producer = new Producer<String, WeatherData>(new ProducerConfig(producerProperties));
        String topic = properties.getProperty("com.iot.app.kafka.topic");
        Random random = new Random();

        LOGGER.info("Sending events");

        while(true) {
            List<WeatherData> weatherDataList = new ArrayList<WeatherData>();
            for(int i = 0; i < numSensors; i++) {
                double temperature = random.nextInt(40 - 30) + 40;
                double precipitation = random.nextInt(4) + 0;
                String[] coords = getCoordinates().split(",");

                for(int j = 0; j < numEvents; j++) {
                    WeatherData weatherData = new WeatherData(UUID.randomUUID().toString(),
                            coords[0],
                            coords[1],
                            new DateTime().getMillis(),
                            temperature,
                            precipitation);
                    weatherDataList.add(weatherData);
                }
            }

            Collections.shuffle(weatherDataList);
            for(WeatherData weatherData : weatherDataList) {
                KeyedMessage<String, WeatherData> keyedMessage = new KeyedMessage<String, WeatherData>(topic, weatherData);
                producer.send(keyedMessage);
                try {
                    Thread.sleep(random.nextInt(3000 - 1000) + 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private String getCoordinates() {
        Random rand = new Random();
        int latPrefix = 33;
        int longPrefix = -96;

        return String.format("%s,%s", latPrefix + rand.nextFloat(), longPrefix + rand.nextFloat());
    }
}
