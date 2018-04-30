package misc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import datamodel.WeatherNotificationData;

public class Test {
    public static void main(String... args) {
        WeatherNotificationData weatherNotificationData = new WeatherNotificationData(
                "weather",
                "A",
                44.9763374,
                93.2353122,
                67.0,
                15.0,
                1.0,
                String.format("%s%s", "Bless Thor.  ", 1));

        ObjectMapper mapper = new ObjectMapper();
        try {
            System.out.printf("\nJSON string is: \n%s", mapper.writeValueAsString(weatherNotificationData));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

    }
}
