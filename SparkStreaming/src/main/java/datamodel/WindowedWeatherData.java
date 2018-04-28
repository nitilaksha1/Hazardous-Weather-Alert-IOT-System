package datamodel;

import lombok.*;

import java.io.Serializable;
import java.util.UUID;

@AllArgsConstructor
@Getter
@Setter
@NoArgsConstructor
@ToString
public class WindowedWeatherData implements Serializable{
    private UUID key;
    private String sensorid;
    private Double latitude;
    private Double longitude;
    private Long timestamp;
    private Double temperature;
    private Double windspeed;
    private Double visibility;
}
