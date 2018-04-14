package decoder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import datamodel.CarData;
import kafka.serializer.Decoder;
import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;
import org.apache.log4j.Logger;
import java.io.IOException;

public class CarDataDecoder implements Encoder<CarData>, Decoder<CarData> {
    private static final Logger LOGGER = Logger.getLogger(CarDataDecoder.class);
    private static ObjectMapper objectMapper = new ObjectMapper();

    public CarDataDecoder() {}
    public CarDataDecoder(VerifiableProperties verifiableProperties) {}

    public byte[] toBytes(CarData carData) {
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

    public CarData fromBytes(byte[] byteArray) {
        CarData carData = null;
        try {
            carData = objectMapper.readValue(byteArray, CarData.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return carData;
    }
}
