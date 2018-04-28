package decoder;

import com.fasterxml.jackson.databind.ObjectMapper;
import datamodel.WeatherNotificationData;
import kafka.serializer.Decoder;
import kafka.utils.VerifiableProperties;
import org.apache.log4j.Logger;
import java.io.IOException;

/**
 * Decoder for {@link WeatherNotificationData} class.
 * @author ambuj, niti
 * @version 1.0
 */
public class WeatherNotificationDataDecoder implements Decoder<WeatherNotificationData> {
    private static final Logger LOGGER = Logger.getLogger(WeatherNotificationDataDecoder.class);
    private static ObjectMapper objectMapper = new ObjectMapper();

    public WeatherNotificationDataDecoder() {}
    public WeatherNotificationDataDecoder(VerifiableProperties verifiableProperties) {}

    public WeatherNotificationData fromBytes(byte[] byteArray) {
        WeatherNotificationData weatherNotificationData = null;
        try {
            weatherNotificationData = objectMapper.readValue(byteArray, WeatherNotificationData.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return weatherNotificationData;
    }
}
