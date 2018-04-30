package consumer;

import com.datastax.spark.connector.cql.CassandraConnector;
import datamodel.*;
import decoder.CarDataDecoder;
import decoder.WeatherDataDecoder;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringDecoder;

import org.apache.avro.ipc.specific.Person;
import org.apache.avro.mapred.Pair;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import org.apache.spark.sql.catalyst.*;
import scala.*;
import utils.reader.PropertyFileReader;

import java.lang.Double;
import java.lang.Long;
import java.sql.Timestamp;
import java.util.*;
import java.util.function.Function;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

public class StreamDataConsumer {
    private static final Logger LOGGER = Logger.getLogger(StreamDataConsumer.class);

    //CAR DATA AGGREGATION FUNCTIONS
    /*
    * This function adds up the speed values of a particular car. This is used for average calculation
     */
    private static Tuple2<Double, Integer> add (Tuple2<Double, Integer> v1, Tuple2<Double, Integer> v2) {
        return new Tuple2<>(v1._1 + v2._1, v1._2 + v2._2);
    }

    /*
    * This function converts multiple (key-> value) pairs to one single (key -> value) pair by adding up all
    * the values associated with a single key
     */
    private static Tuple2<String, Tuple2<Double, Integer>> carmapFunc (Tuple2<String, Tuple2<Double, Integer>> t1,
                                                              Tuple2<String, Tuple2<Double, Integer>> t2) {
        return new Tuple2<>(t1._1, add(t1._2, t2._2));
    }

    /*
    * This function manipulates the stream of CarData objects in order to compute average of car speed.
     */
    private void aggregateCarData(JavaDStream<CarData> carDataStream) {
        JavaPairDStream<String, Double> carspeedPair= carDataStream.mapToPair((t) ->
                new Tuple2(t.getUuid(), t.getSpeed()));

        JavaDStream<Tuple2<String, Tuple2<Double, Integer>>> carspeedcountPair =
                carspeedPair.map(x -> new Tuple2(x._1, new Tuple2<>(x._2, 1)));

        JavaDStream<Tuple2<String, Tuple2<Double, Integer>>> speedsumStream2 =
                carspeedcountPair.reduceByWindow(
                        (x,y) -> carmapFunc(x,y),
                        Durations.seconds(15),
                        Durations.seconds(15));

        JavaDStream<Tuple2<String, Double>> avgStream2 = speedsumStream2
                .map(x -> new Tuple2(x._1, x._2._1/x._2._2));

        JavaDStream<CarNotificationData> notificationStream = avgStream2
                .map(x-> new CarNotificationData(UUID.randomUUID(), x._1, x._2));

        //Save car data to cassandra

        notificationStream.foreachRDD(rdd -> {
            CassandraJavaUtil.javaFunctions(rdd)
                    .writerBuilder("car", "car_data",
                            mapToRow(CarNotificationData.class))
                    .saveToCassandra();

        });

        notificationStream.print();
    }

    //WEATHER DATA AGGREGATION FUNCTIONS
    /*
     * This function adds up all the attributes of weather data (temperature, windspeed, visibility).
     * The last value in the tuple is keycount which shows number of values associated with the key
     * i.e the sensorid and precipiation
     */
    private static Tuple6 addWeatherData (Tuple6<Double, Double, Double, Double, Double, Integer> v1,
                                          Tuple6<Double, Double, Double, Double, Double, Integer> v2) {
        return new Tuple6(v1._1(), v1._2(), v1._3() + v2._3(),
                v1._4() + v2._4(),
                v1._5() + v2._5(),
                v1._6() + v2._6());
    }

    /*
     * This function converts multiple (key-> value) pairs to one single (key -> value) pair by adding up all
     * the values associated with a single key
     */
    private static Tuple5<Double, Double, Double, Double, Double> avgWeatherDataFunc
            (Tuple6<Double, Double, Double, Double, Double, Integer> t) {
        return new Tuple5<>(t._1(), t._2(), t._3()/t._6(), t._4()/t._6(), t._5()/t._6());
    }

    /*
    * This function just adds the weather data values for two tuples. This function is called over and over
    * until each item in the stream (during a windows) has been added.
     */
    private static Tuple2<Tuple2<String, Precipitation>,
            Tuple6<Double, Double, Double, Double, Double, Integer>> weatherFunc (
                    Tuple2<Tuple2<String, Precipitation>, Tuple6<Double, Double, Double, Double, Double, Integer>> t1,
                    Tuple2<Tuple2<String, Precipitation>, Tuple6<Double, Double, Double, Double, Double, Integer>> t2
            ) {
        return new Tuple2<Tuple2<String, Precipitation>, Tuple6<Double, Double, Double, Double, Double, Integer>>
                (new Tuple2(t1._1._1, t1._1._2), addWeatherData(t1._2, t2._2));

    }

    private static boolean isBlizzardWeather(Precipitation precp, Double temperature,
                                      Double windSpeed, Double visibility) {

        if (precp == Precipitation.SNOW &&
                temperature >= 10 && temperature <= 15 &&
                windSpeed > 35 &&
                visibility < 0.4)
            return true;

        return false;
    }

    private static boolean isFogWeather(Precipitation precp, Double visibility) {
        if (precp == Precipitation.NORMAL
                && visibility < 2)
            return true;

        return false;
    }

    private static boolean isWindyWeather(Precipitation precp, Double windSpeed) {
        if (precp == Precipitation.NORMAL
                && windSpeed > 40)
            return true;

        return false;
    }

    /*
     * This function gets the weather statistics aggregated for a window period. Based on the rules,
     * it classifies weather among : Blizzard, fog and windy weather. The appropriate message is sent
     * to hazardous_weather topic. For now it is one topic which receives all the updates. Each update
     * Will contain data in the format given by WeatherNotificationData class. Basically every sensor will
     * subscribing to the hazardous_weather topic will receive the notification. Now based on the lat and
     * long the sensor will decide if the update is relevant to it (based on distance). If the update is
     * relevant the sensor will send update to all cars connected to it
     */
    private static void produceWeatherHazardAlert(Properties properties,
                                                  String type,
                                                  String sensorId,
                                                  Double latitude,
                                                  Double longitude,
                                                  Double temperature,
                                                  Double windSpeed,
                                                  Double visibility,
                                                  String weatherAlert) {
        Properties producerProperties = new Properties();
        producerProperties.put("zookeeper.connect",
                properties.getProperty("com.iot.app.kafka.zookeeper"));
        producerProperties.put("metadata.broker.list",
                properties.getProperty("com.iot.app.kafka.brokerlist"));
        producerProperties.put("request.required.acks", "1");
        producerProperties.put("serializer.class", "encoder.WeatherNotificationDataEncoder");

        Producer<String, WeatherNotificationData> producer = new Producer<>
                (new ProducerConfig(producerProperties));
        String topic = properties.getProperty("com.iot.app.kafka.weather.topic");
        Random random = new Random();

        WeatherNotificationData weatherNotificationData= new WeatherNotificationData(type, sensorId, latitude,
                longitude, temperature, windSpeed, visibility, weatherAlert);

        KeyedMessage<String, WeatherNotificationData> keyedMessage = new KeyedMessage<>(topic,
                weatherNotificationData);
        producer.send(keyedMessage);
    }

    private static void sendWeatherNotification(Properties properties,
                                                String type,
                                                String sensorId,
                                                Precipitation precp,
                                                Double latitude,
                                                Double longitude,
                                                Double temperature,
                                                Double windSpeed,
                                                Double visibility) {
        if (isBlizzardWeather(precp, temperature, windSpeed, visibility)) {
            //Send notification to sensor
            produceWeatherHazardAlert(properties,
                    type,
                    sensorId,
                    latitude,
                    longitude,
                    temperature,
                    windSpeed,
                    visibility,
                    "Blizzard conditions ahead! Travel not recommended");

        }
        else if (isFogWeather(precp, visibility)) {
            //Send notification to sensor
            produceWeatherHazardAlert(properties,
                    type,
                    sensorId,
                    latitude,
                    longitude,
                    temperature,
                    windSpeed,
                    visibility,
                    "Thick Fog conditions ahead! Drive slowly");
        }
        else if (isWindyWeather(precp, windSpeed)) {
            //Send notification to sensor
            produceWeatherHazardAlert(properties,
                    type,
                    sensorId,
                    latitude,
                    longitude,
                    temperature,
                    windSpeed,
                    visibility,
                    "Windy conditions ahead! High-rise vehicles should proceed with caution!");
        }
    }

    /*
     * This function manipulates the stream of WeatherData objects in order to compute average of all the
     * weather data attributes
     */
    private void aggregateWeatherData(Properties properties, JavaDStream<WeatherData> weatherDataStream) {
        //Convert (WeatherData -> <(sensorId, Precipitation) -> (temperature, windspeed, visibility, keycount)>)
        //Keycount is maintained so that I can get the denominator for calculating average.
        //For this I just add up all the 1's in the fourth field of the tuple
        JavaPairDStream<Tuple2<String, Precipitation>,
                Tuple6<Double, Double, Double, Double, Double, Integer>>
                weatherDataPair= weatherDataStream.mapToPair((t) ->
                new Tuple2(new Tuple2(t.getSensorId(), t.getPreciptation()),
                        new Tuple6<>(
                                t.getLatitude(),
                                t.getLongitude(),
                                t.getTemperature(),
                                t.getWindSpeed(),
                                t.getVisibility(),
                                1
                                )));

        //Convert <sensorId, Precipitation> -> <lat, long, temperature, windspeed, visibility, keycount>
        //TO
        //<sensorId, Precipitation> -> <sum of temperatures, sum of windspeeds, sum of visiblity, sum of keycounts>
        JavaDStream<Tuple2<Tuple2<String, Precipitation>,
                Tuple6<Double, Double, Double, Double, Double, Integer>>>
                weathersumStream = weatherDataPair.reduceByWindow(
                        (x,y) -> weatherFunc(x,y),
                        Durations.seconds(5),
                        Durations.seconds(5));

        //Convert <<sensorId, Precipitation> -> <sum of temperatures, sum of windspeeds, sum of visiblity, sum of keycounts>
        //TO
        //<sensorId, Precipitation> -> <sum of temperatures/sum of keycount,
        // sum of windspeeds / sum of keycount,
        // sum of visiblity / sum of keycount>
        JavaDStream<Tuple2<Tuple2<String, Precipitation>,
                Tuple5<Double, Double, Double, Double, Double>>> avgWeatherStream =
                weathersumStream
                .map(x -> new Tuple2(x._1, avgWeatherDataFunc(x._2)));


        Long ts = System.currentTimeMillis();

        //Persisting windowed weather data to Cassandra DB
        JavaDStream<WindowedWeatherData> cassandraStream = avgWeatherStream
                .map(x -> new WindowedWeatherData(UUID.randomUUID(),
                        x._1._1,
                        x._2._1(),
                        x._2._2(),
                        ts,
                        x._2._3(),
                        x._2._4(),
                        x._2._5()));

        cassandraStream.foreachRDD(rdd -> {
            CassandraJavaUtil.javaFunctions(rdd)
                    .writerBuilder("weather", "weather_data",
                            mapToRow(WindowedWeatherData.class))
            .saveToCassandra();
        });

        //Analyze each windowed record and send weather notification to sensor
        //Now since weather updates happen per window it makes sense for sensor also
        //to broadcast weather update for duration of a window
        avgWeatherStream.foreachRDD((rdd) -> {
            List<Tuple2<Tuple2<String, Precipitation>, Tuple5<Double, Double, Double, Double, Double>>>
                    avgWeatherRecord = rdd.collect();

            for (Tuple2<Tuple2<String, Precipitation>, Tuple5<Double, Double, Double, Double, Double>>
                    record : avgWeatherRecord) {
                //Get the key which is <sensorId, Precipitation>
                Tuple2<String, Precipitation> key = record._1;
                //Get the value which is <lat, long, avg temp, avg wind speed, avg visibility>
                Tuple5<Double, Double, Double, Double, Double> value = record._2;

                sendWeatherNotification(properties,
                        "weather",
                        key._1,
                        key._2,
                        value._1(),
                        value._2(),
                        value._3(),
                        value._4(),
                        value._5());
            }
        });


        //Print average weather statistics for each sensorId
        avgWeatherStream.print();

    }

    /*
    * This function is the start point of the Spark Application that consumes and analyzes weather and
    * car data.
     */
    public void consume() {
        Properties properties = PropertyFileReader.readConsumerPropertyFile();
        Properties prodProperties = PropertyFileReader.readProducerPropertyFile();
        SparkConf sparkConf = new SparkConf()
                .setAppName(properties.getProperty("com.iot.app.spark.app.name"))
                .setMaster(properties.getProperty("com.iot.app.spark.master"))
                .set("spark.streaming.concurrentJobs", "4")
                .set("spark.cassandra.connection.host", "localhost");

        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));
        javaStreamingContext.checkpoint(properties.getProperty("com.iot.app.spark.checkpoint.dir"));

        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("zookeeper.connect", properties.getProperty("com.iot.app.kafka.zookeeper"));
        kafkaParams.put("metadata.broker.list", properties.getProperty("com.iot.app.kafka.brokerlist"));

        String topic = properties.getProperty("com.iot.app.kafka.car.topic");
        Set<String> carTopicSet = new HashSet<String>();
        carTopicSet.add(topic);

        JavaPairInputDStream<String, CarData> carKafkaStream = KafkaUtils.createDirectStream(
                javaStreamingContext,
                String.class,
                CarData.class,
                StringDecoder.class,
                CarDataDecoder.class,
                kafkaParams,
                carTopicSet
        );

        LOGGER.info("Started car stream processing...");
        JavaDStream<CarData> nonFilteredCarDataStream = carKafkaStream.map(tuple -> tuple._2());
        aggregateCarData(nonFilteredCarDataStream);

        String weatherTopic = properties.getProperty("com.iot.app.kafka.weather.topic");
        Set<String> weatherTopicSet = new HashSet<String>();
        weatherTopicSet.add(weatherTopic);

        JavaPairInputDStream<String, WeatherData> weatherKafkaStream = KafkaUtils.createDirectStream(
                javaStreamingContext,
                String.class,
                WeatherData.class,
                StringDecoder.class,
                WeatherDataDecoder.class,
                kafkaParams,
                weatherTopicSet
        );

        LOGGER.info("Started weather stream processing...");
        JavaDStream<WeatherData> nonFilteredWeatherDataStream = weatherKafkaStream.map(tuple -> tuple._2());
        aggregateWeatherData(prodProperties, nonFilteredWeatherDataStream);
        //produceCarNotifications(nonFilteredDataStream, prodProperties);

        javaStreamingContext.start();
        try {
            javaStreamingContext.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void produceCarNotifications(JavaDStream<CarData> dStream, Properties properties) {

        dStream.foreachRDD(
                rdd -> {
                    for (CarData carData : rdd.collect()) {
                        Properties producerProperties = new Properties();
                        producerProperties.put("zookeeper.connect",
                                properties.getProperty("com.iot.app.kafka.zookeeper"));
                        producerProperties.put("metadata.broker.list",
                                properties.getProperty("com.iot.app.kafka.brokerlist"));
                        producerProperties.put("request.required.acks", "1");
                        producerProperties.put("serializer.class", "encoder.CarNotificationDataEncoder");

                        Producer<String, CarNotificationData> producer = new Producer<>
                                (new ProducerConfig(producerProperties));
                        String topic = properties.getProperty("com.iot.app.kafka.car.topic");
                        Random random = new Random();

                        CarNotificationData carNotificationData = new CarNotificationData(null, carData.getUuid(),
                                carData.getSpeed());

                        KeyedMessage<String, CarNotificationData> keyedMessage = new KeyedMessage<>(topic,
                                carNotificationData);
                        producer.send(keyedMessage);
                    }
                }
        );
    }
}
