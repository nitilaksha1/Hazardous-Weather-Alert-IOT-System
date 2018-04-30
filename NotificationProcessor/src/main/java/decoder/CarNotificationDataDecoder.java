package decoder;

import com.fasterxml.jackson.databind.ObjectMapper;
import datamodel.CarData;
import kafka.serializer.Decoder;
import kafka.utils.VerifiableProperties;
import org.apache.log4j.Logger;
import java.io.IOException;

public class CarNotificationDataDecoder implements Decoder<CarData> {
    private static final Logger LOGGER = Logger.getLogger(CarNotificationDataDecoder.class);
    private static ObjectMapper objectMapper = new ObjectMapper();

    public CarNotificationDataDecoder() {}
    public CarNotificationDataDecoder(VerifiableProperties verifiableProperties) {}

    public CarData fromBytes(byte[] byteArray) {
        CarData carNotificationData = null;
        try {
            carNotificationData = objectMapper.readValue(byteArray, CarData.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return carNotificationData;
    }
}
