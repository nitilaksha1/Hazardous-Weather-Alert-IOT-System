package encoder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import datamodel.CarData;
import datamodel.CarNotificationData;
import kafka.serializer.Decoder;
import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;
import org.apache.log4j.Logger;
import java.io.IOException;

public class CarNotificationDataEncoder implements Encoder<CarNotificationData>{
    private static final Logger LOGGER = Logger.getLogger(CarNotificationDataEncoder.class);
    private static ObjectMapper objectMapper = new ObjectMapper();

    public CarNotificationDataEncoder() {}

    public CarNotificationDataEncoder(VerifiableProperties verifiableProperties) {}

    public byte[] toBytes(CarNotificationData carData) {
        byte[] byteArray = null;
        try {
            String objectAsString = objectMapper.writeValueAsString(carData);
            LOGGER.info(objectAsString);
            byteArray = objectAsString.getBytes();
            return byteArray;
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return byteArray;
    }
}
