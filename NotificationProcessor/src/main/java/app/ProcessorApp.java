package app;

import processor.WeatherNotificationProcessor;
import utils.reader.PropertyFileReader;

public class ProcessorApp {
    public static void main(String... args) {
        double latitude = Double.parseDouble(args[0]);
        double longitude = Double.parseDouble(args[1]);

        WeatherNotificationProcessor weatherNotificationProcessor =
                new WeatherNotificationProcessor(PropertyFileReader.readPropertyFile(), latitude, longitude);
        weatherNotificationProcessor.processNotificationEvent();
    }
}
