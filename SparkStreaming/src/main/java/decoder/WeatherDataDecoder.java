package decoder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import datamodel.WeatherData;
import kafka.serializer.Decoder;
import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;
import org.apache.log4j.Logger;

import java.io.IOException;

public class WeatherDataDecoder implements Encoder<WeatherData>, Decoder<WeatherData> {
    private static final Logger LOGGER = Logger.getLogger(WeatherDataDecoder.class);
    private static ObjectMapper objectMapper = new ObjectMapper();

    public WeatherDataDecoder(){}

    public WeatherDataDecoder(VerifiableProperties verifiableProperties) {}

    public byte[] toBytes(WeatherData weatherData) {
        byte[] byteArray = null;
        try {
            String objectAsString = objectMapper.writeValueAsString(weatherData);
            LOGGER.info(objectAsString);
            byteArray = objectAsString.getBytes();
            return byteArray;
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return byteArray;
    }

    public WeatherData fromBytes(byte[] byteArray) {
        WeatherData weatherData = null;
        try {
            weatherData = objectMapper.readValue(byteArray, WeatherData.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return weatherData;
    }
}
