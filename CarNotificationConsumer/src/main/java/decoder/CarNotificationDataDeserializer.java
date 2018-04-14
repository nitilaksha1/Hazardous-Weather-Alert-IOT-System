package decoder;

import datamodel.CarNotificationData;
import org.apache.kafka.common.serialization.Deserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;

public class CarNotificationDataDeserializer implements Deserializer<CarNotificationData> {
    public void close() {
    }

    public void configure(Map<String, ?> arg0, boolean arg1) {
    }

    public CarNotificationData deserialize(String arg0, byte[] arg1) {
        ObjectMapper mapper = new ObjectMapper();
        CarNotificationData notificationData = null;
        try {
            notificationData = mapper.readValue(arg1, CarNotificationData.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return notificationData;
    }
}
