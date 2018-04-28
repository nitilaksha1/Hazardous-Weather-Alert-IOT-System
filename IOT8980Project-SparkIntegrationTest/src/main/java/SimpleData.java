import lombok.*;

import java.io.Serializable;

@AllArgsConstructor
@Getter
@Setter
@NoArgsConstructor
@ToString

public class SimpleData implements Serializable {
    private Double key;
    private Double value;
}
