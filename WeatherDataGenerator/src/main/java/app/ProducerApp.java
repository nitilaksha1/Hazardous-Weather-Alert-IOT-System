package app;

import producer.WeatherDataProducer;
import utils.reader.PropertyFileReader;

public class ProducerApp {
    public static void main(String... args) {
        WeatherDataProducer weatherDataProducer = new WeatherDataProducer(PropertyFileReader.readPropertyFile());
        weatherDataProducer.generateIotEvent(1, 5);
    }
}