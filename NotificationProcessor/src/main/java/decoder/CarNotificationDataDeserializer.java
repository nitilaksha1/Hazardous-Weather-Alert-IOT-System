package decoder;

import datamodel.CarData;
import org.apache.kafka.common.serialization.Deserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;

public class CarNotificationDataDeserializer implements Deserializer<CarData> {
    public void close() {
    }

    public void configure(Map<String, ?> arg0, boolean arg1) {
    }

    public CarData deserialize(String arg0, byte[] arg1) {
        ObjectMapper mapper = new ObjectMapper();
        CarData notificationData = null;
        try {
            notificationData = mapper.readValue(arg1, CarData.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return notificationData;
    }
}
