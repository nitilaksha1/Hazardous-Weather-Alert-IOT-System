package datamodel;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.joda.time.DateTime;
import java.io.Serializable;

@AllArgsConstructor
@Getter
public class WeatherData implements Serializable {
    private String stationId;
    private String latitude;
    private String longitude;
    private DateTime timestamp;
    private double temperature;
    private double preciptation;
}
