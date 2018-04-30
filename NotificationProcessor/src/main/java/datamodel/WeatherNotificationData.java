package datamodel;

import lombok.*;
import java.io.Serializable;

@AllArgsConstructor
@Getter
@Setter
@NoArgsConstructor
@ToString
/**
 * WeatherNotificationData class.
 * @author ambuj, niti
 * @version 1.0
 */
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