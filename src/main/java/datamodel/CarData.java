package datamodel;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.joda.time.DateTime;
import java.io.Serializable;

@AllArgsConstructor
@Getter
public class CarData implements Serializable {
    private String carId;
    private String latitude;
    private String longitude;
    private DateTime timestamp;
    private double speed;
    private double fuelLevel;
}
