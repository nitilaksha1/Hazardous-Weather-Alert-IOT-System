package producer;

import datamodel.CarData;
import kafka.producer.KeyedMessage;
import lombok.AllArgsConstructor;
import org.apache.log4j.Logger;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import org.joda.time.DateTime;
import java.util.*;

@AllArgsConstructor
public class CarDataProducer {
    private static final Logger LOGGER = Logger.getLogger(CarDataProducer.class);
    private Properties properties;

    public void generateIotEvent(int numCars, int numEvents) {
        Properties producerProperties = new Properties();
        producerProperties.put("zookeeper.connect", properties.getProperty("com.iot.app.kafka.zookeeper"));
        producerProperties.put("metadata.broker.list", properties.getProperty("com.iot.app.kafka.brokerlist"));
        producerProperties.put("request.required.acks", "1");
        producerProperties.put("serializer.class", "encoder.CarDataEncoder");

        Producer<String, CarData> producer = new Producer<String, CarData>(new ProducerConfig(producerProperties));
        String topic = properties.getProperty("com.iot.app.kafka.topic");
        Random random = new Random();

        LOGGER.info("Sending events");

        while(true) {
            List<CarData> carDataList = new ArrayList<CarData>();
            for(int i = 0; i < numCars; i++) {
                double speed = random.nextInt(100 - 20) + 20;
                double fuelLevel = random.nextInt(40 - 10) + 10;

                for(int j = 0; j < numEvents; j++) {
                    String[] coords = getCoordinates().split(",");
                    CarData carData = new CarData(UUID.randomUUID().toString(),
                            coords[0],
                            coords[1],
                            new DateTime(),
                            speed,
                            fuelLevel);
                    carDataList.add(carData);
                }
            }
            Collections.shuffle(carDataList);
            for(CarData carData : carDataList) {
                KeyedMessage<String, CarData> keyedMessage = new KeyedMessage<String, CarData>(topic, carData);
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
