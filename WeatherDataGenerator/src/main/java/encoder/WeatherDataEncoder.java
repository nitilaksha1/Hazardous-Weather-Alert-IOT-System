package encoder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import datamodel.WeatherData;
import kafka.serializer.Decoder;
import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;
import org.apache.log4j.Logger;
import java.io.IOException;

public class WeatherDataEncoder implements Encoder<WeatherData>, Decoder<WeatherData>{
    private static final Logger LOGGER = Logger.getLogger(WeatherDataEncoder.class);
    private static ObjectMapper objectMapper = new ObjectMapper();

    public WeatherDataEncoder(){}

    public WeatherDataEncoder(VerifiableProperties verifiableProperties) {}

    public byte[] toBytes(WeatherData carData) {
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

    public WeatherData fromBytes(byte[] byteArray) {
        WeatherData carData = null;
        try {
            carData = objectMapper.readValue(byteArray, WeatherData.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return carData;
    }
}
