package app;

import com.google.inject.AbstractModule;
import lombok.AllArgsConstructor;
import simulator.*;
import utils.reader.PropertyFileReader;

import java.util.Properties;

@AllArgsConstructor
public class ProducerAppModule extends AbstractModule {
    private String simulatorType;
    private double latitude;
    private double longitude;

    protected void configure() {
        Properties properties = PropertyFileReader.readPropertyFile();
        switch (simulatorType) {
            case "blizzard":
                bind(WeatherDataSimulator.class).toInstance(new BlizzardSimulator(properties, latitude, longitude));
                break;
            case "fog":
                bind(WeatherDataSimulator.class).toInstance(new FogSimulator(properties, latitude, longitude));
                break;
            case "wind":
                bind(WeatherDataSimulator.class).toInstance(new WindAdvisorySimulator(properties, latitude, longitude));
                break;
            default:
                bind(WeatherDataSimulator.class).toInstance(new NormalWeatherSimulator(properties, latitude, longitude));
                break;
        }

    }
}
