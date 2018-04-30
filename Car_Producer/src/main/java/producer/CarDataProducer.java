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
        String [] carIds = {"car-1", "car-2", "car-3", "car-4", "car-5"};
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
            String[] latArr = new String[]
                    {"44.930607", "44.923924", "44.934099", "44.942482", "44.9433",
                            "44.947935", "44.963715", "44.965505", "44.977936"};

            String[] longArr = new String[]
                    {"-93.348760", "-93.375711", "-93.337774", "-93.326616", "-93.3212",
                            "-93.301365", "-93.290223", "-93.266233", "-93.256577"};

            List<CarData> carDataList = new ArrayList<CarData>();
            for(int i = 0; i < numCars; i++) {
                double speed = random.nextInt(100 - 20) + 20;
                double fuelLevel = random.nextInt(40 - 10) + 10;

                for(int j = 0; j < 9; j++) {
                    CarData carData = new CarData("car",
                            carIds[random.nextInt(4)],
                            latArr[i],
                            longArr[i],
                            new DateTime().getMillis(),
                            speed,
                            fuelLevel);
                    carDataList.add(carData);
                }
            }
            //Collections.shuffle(carDataList);
            for(CarData carData : carDataList) {
                KeyedMessage<String, CarData> keyedMessage = new KeyedMessage<String, CarData>(topic, carData);
                producer.send(keyedMessage);
                try {
                    Thread.sleep(1500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
