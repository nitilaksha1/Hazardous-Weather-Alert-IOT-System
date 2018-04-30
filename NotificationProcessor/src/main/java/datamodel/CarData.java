package datamodel;

import lombok.*;
import java.io.Serializable;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
public class CarData implements Serializable {
    private String type;
    private String uuid;
    private String latitude;
    private String longitude;
    private Long timestamp;
    private double speed;
    private double fuelLevel;
}
