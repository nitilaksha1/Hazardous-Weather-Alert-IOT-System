package datamodel;

import lombok.*;

import java.io.Serializable;
import java.util.UUID;

@AllArgsConstructor
@Getter
@Setter
@NoArgsConstructor
@ToString
public class CarNotificationData implements Serializable {
    private UUID key;
    private String carid;
    private Double speed;
}
