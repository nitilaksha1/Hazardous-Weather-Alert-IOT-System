package encoder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import datamodel.CarData;
import datamodel.CarNotificationData;
import datamodel.WeatherNotificationData;
import kafka.serializer.Decoder;
import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;
import org.apache.log4j.Logger;
import java.io.IOException;

public class WeatherNotificationDataEncoder implements Encoder<WeatherNotificationData>{
    private static final Logger LOGGER = Logger.getLogger(WeatherNotificationDataEncoder.class);
    private static ObjectMapper objectMapper = new ObjectMapper();

    public WeatherNotificationDataEncoder() {}

    public WeatherNotificationDataEncoder(VerifiableProperties verifiableProperties) {}

    public byte[] toBytes(WeatherNotificationData weatherNotificationData) {
        byte[] byteArray = null;
        try {
            String objectAsString = objectMapper.writeValueAsString(weatherNotificationData);
            LOGGER.info(objectAsString);
            byteArray = objectAsString.getBytes();
            return byteArray;
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return byteArray;
    }
}
