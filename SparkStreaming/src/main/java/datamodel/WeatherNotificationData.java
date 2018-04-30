package datamodel;

import lombok.*;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.UUID;

@AllArgsConstructor
@Getter
@Setter
@NoArgsConstructor
@ToString
public class WeatherNotificationData implements Serializable {
    private String type;
    private String sensorId;
    private Double latitude;
    private Double longitude;
    private Double temperature;
    private Double windspeed;
    private Double visibility;
    private String weatherAlert;
}
