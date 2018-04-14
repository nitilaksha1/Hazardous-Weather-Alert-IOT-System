package utils.reader;

import org.apache.log4j.Logger;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertyFileReader {
    private static final Logger LOGGER = Logger.getLogger(PropertyFileReader.class);
    private static Properties properties = new Properties();

    public static Properties readPropertyFile() {
        if(properties.isEmpty()) {
            InputStream inputStream = PropertyFileReader.class
                    .getClassLoader()
                    .getResourceAsStream("iot-spark.properties");
            try {
                properties.load(inputStream);
            } catch (IOException e) {
                LOGGER.error(e);
                e.printStackTrace();
            } finally {
                if(inputStream != null) {
                    try {
                        inputStream.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return properties;
    }
}
