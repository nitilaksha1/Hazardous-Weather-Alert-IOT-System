package decoder;

import com.fasterxml.jackson.databind.ObjectMapper;
import datamodel.WeatherNotificationData;
import java.util.Map;

/**
 * Deserializer for {@link WeatherNotificationData} class.
 * @author ambuj, niti
 * @version 1.0
 */
public class WeatherNotificationDataDeserializer {
    public void close() {
    }

    public void configure(Map<String, ?> arg0, boolean arg1) {
    }

    public WeatherNotificationData deserialize(String arg0, byte[] arg1) {
        ObjectMapper mapper = new ObjectMapper();
        WeatherNotificationData weatherNotificationData = null;
        try {
            weatherNotificationData = mapper.readValue(arg1, WeatherNotificationData.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return weatherNotificationData;
    }
}