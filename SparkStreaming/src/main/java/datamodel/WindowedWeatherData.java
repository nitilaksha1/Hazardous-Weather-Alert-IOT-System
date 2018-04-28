package datamodel;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import java.io.Serializable;
import java.util.UUID;

public class WindowedWeatherData {
    private UUID key;
    private String sensorId;
    private double latitude;
    private double longitude;
    private Long timestamp;
    private double temperature;
    private Precipitation preciptation;
    private double windSpeed;
    private double visibility;
}
