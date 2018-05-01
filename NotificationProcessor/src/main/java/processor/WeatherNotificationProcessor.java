package processor;

// http://makemobiapps.blogspot.com/p/multiple-client-server-chat-programming.html
import datamodel.CarData;
import datamodel.WeatherNotificationData;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

/**
 * Process weather data notifications.
 * @author ambuj, niti.
 * @version 1.0.
 */
@AllArgsConstructor
public class WeatherNotificationProcessor {
    private static final Logger LOGGER = Logger.getLogger(WeatherNotificationProcessor.class);
    private static final int PORT = 9007;
    private static final List<ClientHandler> socketList =
            Collections.synchronizedList(new ArrayList<ClientHandler>());
    private static final Properties consumerProps = new Properties();
    private static final Properties carProps = new Properties();

    private Properties properties;
    private double latitude;
    private double longitude;

    public void processNotificationEvent() {
        setupConsumerPropertiesV1();
        setupConsumerPropertiesV2();
        final Consumer weatherConsumer = new KafkaConsumer<String, WeatherNotificationData>(consumerProps);
        final Consumer carConsumer = new KafkaConsumer<String, WeatherNotificationData>(carProps);
        weatherConsumer.subscribe(Arrays.asList(properties.getProperty("com.iot.app.kafka.topic-1")));
        carConsumer.subscribe(Arrays.asList(properties.getProperty("com.iot.app.kafka.topic-2")));

        // create a thread to accept incoming client connections.
        Thread connectionThread = new Thread() {
            public void run() {
                try {
                    ServerSocket serverSocket = new ServerSocket(PORT);
                    while(true) {
                        Socket clientSocket = serverSocket.accept();
                        socketList.add(new ClientHandler(clientSocket));
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };

        // create a thread to read from kafka topic
        Thread consumerThread = new Thread() {
            public void run() {
                while (true) {
                    // do we need to set up poll timeout here?
                    ConsumerRecords records = weatherConsumer.poll(0);
                    List<WeatherNotificationData> weatherNotificationDataList =
                            new ArrayList<WeatherNotificationData>();
                    for (Object record : records) {
                        ConsumerRecord consumerRecord = (ConsumerRecord) record;
                        WeatherNotificationData weatherNotificationData =
                                (WeatherNotificationData) consumerRecord.value();
                        weatherNotificationDataList.add(weatherNotificationData);
                    }
                    synchronized (socketList) {
                        for(ClientHandler clientHandler : socketList) {
                            NotificationHandler notificationHandler = new NotificationHandler(
                                    clientHandler, weatherNotificationDataList, 44.97, -93.26);
                            notificationHandler.run();
                        }
                    }
                }
            }
        };

        // create a thread to read from kafka topic
        Thread carThread = new Thread() {
            public void run() {
                while (true) {
                    // do we need to set up poll timeout here?
                    ConsumerRecords records = carConsumer.poll(0);
                    List<CarData> carDataList =
                            new ArrayList<CarData>();
                    for (Object record : records) {
                        ConsumerRecord consumerRecord = (ConsumerRecord) record;
                        CarData carData =
                                (CarData) consumerRecord.value();
                        carDataList.add(carData);
                    }
                    synchronized (socketList) {
                        for(ClientHandler clientSocket : socketList) {
                            CarNotificationHandler carNotificationHandler = new CarNotificationHandler(
                                    clientSocket, carDataList);
                            carNotificationHandler.run();
                        }
                    }
                }
            }
        };

        connectionThread.start();
        consumerThread.start();
        carThread.start();
    }

    private void setupConsumerPropertiesV1() {
        consumerProps.put("bootstrap.servers", properties.getProperty("com.iot.app.kafka.brokerlist"));
        consumerProps.put("group.id", properties.getProperty("com.iot.app.kafka.consumer.groupid"));
        consumerProps.put("enable.auto.commit", "true");
        //consumerProps.put("auto.offset.reset", "earliest");
        consumerProps.put("auto.commit.interval.ms", "1000");
        consumerProps.put("session.timeout.ms", "30000");
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "decoder.WeatherNotificationDataDeserializer");
        consumerProps.put("deserializer.class", "decoder.WeatherNotificationDataDecoder");
        //consumerProps.put("auto.offset.reset", "earliest");
    }

    private void setupConsumerPropertiesV2() {
        carProps.put("bootstrap.servers", properties.getProperty("com.iot.app.kafka.brokerlist"));
        carProps.put("group.id", properties.getProperty("com.iot.app.kafka.consumer.groupid"));
        carProps.put("enable.auto.commit", "true");
        //carProps.put("auto.offset.reset", "earliest");
        carProps.put("auto.commit.interval.ms", "1000");
        carProps.put("session.timeout.ms", "30000");
        carProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        carProps.put("value.deserializer", "decoder.CarNotificationDataDeserializer");
        carProps.put("deserializer.class", "decoder.CarNotificationDataDecoder");
        //carProps.put("auto.offset.reset", "earliest");
    }
}

