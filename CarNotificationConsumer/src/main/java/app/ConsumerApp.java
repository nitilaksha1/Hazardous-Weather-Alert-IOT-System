package app;

import consumer.CarNotificationConsumer;
import utils.reader.PropertyFileReader;

public class ConsumerApp {
    public static void main(String... args) {
        CarNotificationConsumer carNotificationConsumer =
                new CarNotificationConsumer(PropertyFileReader.readPropertyFile());
        carNotificationConsumer.consumeNotificationEvent();
    }
}
