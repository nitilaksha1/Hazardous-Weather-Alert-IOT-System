package app;

import simulator.BlizzardSimulator;
import simulator.WeatherDataSimulator;
import utils.reader.PropertyFileReader;

public class ProducerApp {
    public static void main(String... args) {
        WeatherDataSimulator weatherDataSimulator
                = new BlizzardSimulator(PropertyFileReader.readPropertyFile(), 33, -96);
        weatherDataSimulator.generateWeatherEvent();
    }
}