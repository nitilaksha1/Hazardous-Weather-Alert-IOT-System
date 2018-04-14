package decoder;

import com.fasterxml.jackson.databind.ObjectMapper;
import datamodel.CarNotificationData;
import kafka.serializer.Decoder;
import kafka.utils.VerifiableProperties;
import org.apache.log4j.Logger;
import java.io.IOException;

public class CarNotificationDataDecoder implements Decoder<CarNotificationData> {
    private static final Logger LOGGER = Logger.getLogger(CarNotificationDataDecoder.class);
    private static ObjectMapper objectMapper = new ObjectMapper();

    public CarNotificationDataDecoder() {}
    public CarNotificationDataDecoder(VerifiableProperties verifiableProperties) {}

    public CarNotificationData fromBytes(byte[] byteArray) {
        CarNotificationData carNotificationData = null;
        try {
            carNotificationData = objectMapper.readValue(byteArray, CarNotificationData.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return carNotificationData;
    }

}
