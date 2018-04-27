package app;

import com.google.inject.Guice;
import com.google.inject.Injector;
import simulator.WeatherDataSimulator;

public class ProducerApp {
    public static void main(String... args) {
        String simulator_type = args[0];
        double latitude = Double.parseDouble(args[1]);
        double longitude = Double.parseDouble(args[2]);

        Injector injector = Guice.createInjector(new ProducerAppModule(simulator_type, latitude, longitude));

        WeatherDataSimulator weatherDataSimulator = injector.getInstance(WeatherDataSimulator.class);
        weatherDataSimulator.generateWeatherEvent();
    }
}