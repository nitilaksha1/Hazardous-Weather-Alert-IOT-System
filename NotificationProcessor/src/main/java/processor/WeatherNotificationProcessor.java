package processor;

// http://makemobiapps.blogspot.com/p/multiple-client-server-chat-programming.html
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
    private static final List<Socket> socketList =
            Collections.synchronizedList(new ArrayList<Socket>());
    private static final Properties consumerProps = new Properties();

    private Properties properties;
    private double latitude;
    private double longitude;

    public void processNotificationEvent() {
        setupConsumerProperties();
        final Consumer consumer = new KafkaConsumer<String, WeatherNotificationData>(consumerProps);
        consumer.subscribe(Arrays.asList(properties.getProperty("com.iot.app.kafka.topic")));

        // create a thread to accept incoming connections.
        Thread connectionThread = new Thread() {
            public void run() {
                try {
                    ServerSocket serverSocket = new ServerSocket(PORT);
                    while(true) {
                        Socket clientSocket = serverSocket.accept();
                        socketList.add(clientSocket);
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
                    ConsumerRecords records = consumer.poll(0);
                    List<WeatherNotificationData> weatherNotificationDataList =
                            new ArrayList<WeatherNotificationData>();
                    for (Object record : records) {
                        ConsumerRecord consumerRecord = (ConsumerRecord) record;
                        WeatherNotificationData weatherNotificationData =
                                (WeatherNotificationData) consumerRecord.value();
                        weatherNotificationDataList.add(weatherNotificationData);
                    }
                    synchronized (socketList) {
                        for(Socket clientSocket : socketList) {
                            NotificationHandler notificationHandler = new NotificationHandler(
                                    clientSocket, weatherNotificationDataList, latitude, longitude);
                            Thread notificationThread = new Thread(notificationHandler);
                            notificationThread.start();
                        }
                    }
                }
            }
        };

        connectionThread.start();
        consumerThread.start();
    }

    private void setupConsumerProperties() {
        consumerProps.put("bootstrap.servers", properties.getProperty("com.iot.app.kafka.brokerlist"));
        consumerProps.put("group.id", properties.getProperty("com.iot.app.kafka.consumer.groupid"));
        consumerProps.put("enable.auto.commit", "true");
        consumerProps.put("auto.offset.reset", "earliest");
        consumerProps.put("auto.commit.interval.ms", "1000");
        consumerProps.put("session.timeout.ms", "30000");
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "decoder.WeatherNotificationDataDeserializer");
        consumerProps.put("deserializer.class", "decoder.WeatherNotificationDataDecoder");
        consumerProps.put("auto.offset.reset", "earliest");
    }
}

