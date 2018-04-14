package utils.reader;

import jdk.internal.util.xml.impl.Input;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertyFileReader {
    private static final Logger LOGGER = Logger.getLogger(PropertyFileReader.class);
    private static Properties sparkProperties = new Properties();
    private static Properties kafkaProperties = new Properties();

    public static Properties readConsumerPropertyFile() {
        if(sparkProperties.isEmpty()) {

            InputStream inputStream = PropertyFileReader.class
                        .getClassLoader()
                        .getResourceAsStream("iot-spark.properties");
            try {
                sparkProperties.load(inputStream);
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
        return sparkProperties;
    }

    public static Properties readProducerPropertyFile() {
        if(kafkaProperties.isEmpty()) {

            InputStream inputStream = PropertyFileReader.class
                    .getClassLoader()
                    .getResourceAsStream("iot-kafka.properties");
            try {
                kafkaProperties.load(inputStream);
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
        return kafkaProperties;
    }
}
