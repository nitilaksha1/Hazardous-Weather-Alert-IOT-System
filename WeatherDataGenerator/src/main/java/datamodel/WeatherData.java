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
    private String stationId;
    private String latitude;
    private String longitude;
    private Long timestamp;
    private double temperature;
    private double preciptation;
}
