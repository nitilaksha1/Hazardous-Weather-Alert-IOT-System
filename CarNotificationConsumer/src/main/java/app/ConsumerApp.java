package app;

import consumer.CarNotificationConsumer;
import lombok.AllArgsConstructor;
import org.apache.log4j.Logger;
import utils.reader.PropertyFileReader;

import java.util.Properties;


public class ConsumerApp {
    public static void main(String... args) {
        CarNotificationConsumer carNotificationConsumer = new CarNotificationConsumer(PropertyFileReader.readPropertyFile());
        carNotificationConsumer.consumeNotificationEvent();
    }
}
