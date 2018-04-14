package datamodel;

import lombok.*;

import java.io.Serializable;

@AllArgsConstructor
@Getter
@Setter
@NoArgsConstructor
@ToString
public class CarData implements Serializable {
    private String carId;
    private String latitude;
    private String longitude;
    private Long timestamp;
    private double speed;
    private double fuelLevel;
}
