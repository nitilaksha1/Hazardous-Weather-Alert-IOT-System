package datamodel;

import lombok.*;
import java.io.Serializable;

@AllArgsConstructor
@Getter
@Setter
@NoArgsConstructor
@ToString
public class CarNotificationData implements Serializable {
    private String carId;
    private double speed;
}
