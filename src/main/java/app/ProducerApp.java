package app;

import producer.CarDataProducer;
import utils.reader.PropertyFileReader;

public class ProducerApp {
    public static void main(String... args) {
        CarDataProducer carDataProducer = new CarDataProducer(PropertyFileReader.readPropertyFile());
        carDataProducer.generateIotEvent(1, 5);
    }
}
