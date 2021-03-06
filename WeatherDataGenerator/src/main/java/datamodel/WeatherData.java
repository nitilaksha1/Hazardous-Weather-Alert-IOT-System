package datamodel;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import java.io.Serializable;

@AllArgsConstructor
@Getter
@Setter
@NoArgsConstructor
public class WeatherData implements Serializable {
    private String sensorId;
    private double latitude;
    private double longitude;
    private Long timestamp;
    private double temperature;
    private Precipitation preciptation;
    private double windSpeed;
    private double visibility;
}
