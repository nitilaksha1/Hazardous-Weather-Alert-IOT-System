package datamodel;

import lombok.*;

import java.io.Serializable;

@AllArgsConstructor
@Getter
@Setter
@NoArgsConstructor
@ToString
public class WeatherNotificationData implements Serializable {
    private Double latitude;
    private Double longitude;
    private Double temperature;
    private Double windSpeed;
    private Double visibility;
    private String weatherAlert;
}
