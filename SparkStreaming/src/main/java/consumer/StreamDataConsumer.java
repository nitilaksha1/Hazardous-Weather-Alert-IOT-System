package consumer;

import datamodel.CarData;
import datamodel.CarNotificationData;
import datamodel.Precipitation;
import datamodel.WeatherData;
import decoder.CarDataDecoder;
import decoder.WeatherDataDecoder;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringDecoder;
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
import scala.*;
import utils.reader.PropertyFileReader;

import java.lang.Double;
import java.util.*;
import java.util.function.Function;

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
                new Tuple2(t.getCarId(), t.getSpeed()));

        JavaDStream<Tuple2<String, Tuple2<Double, Integer>>> carspeedcountPair =
                carspeedPair.map(x -> new Tuple2(x._1, new Tuple2<>(x._2, 1)));

        JavaDStream<Tuple2<String, Tuple2<Double, Integer>>> speedsumStream =
                carspeedcountPair.reduce((x,y) -> carmapFunc(x,y));

        JavaDStream<Tuple2<String, Tuple2<Double, Integer>>> speedsumStream2 =
                carspeedcountPair.reduceByWindow(
                        (x,y) -> carmapFunc(x,y),
                        Durations.seconds(15),
                        Durations.seconds(15));

        JavaDStream<Tuple2<String, Double>> avgStream = speedsumStream
                .map(x -> new Tuple2(x._1, x._2._1/x._2._2));

        JavaDStream<Tuple2<String, Double>> avgStream2 = speedsumStream2
                .map(x -> new Tuple2(x._1, x._2._1/x._2._2));

        avgStream2.print();
    }

    //WEATHER DATA AGGREGATION FUNCTIONS
    /*
     * This function adds up all the attributes of weather data (temperature, windspeed, visibility).
     * The last value in the tuple is keycount which shows number of values associated with the key
     * i.e the sensorid and precipiation
     */
    private static Tuple4 addWeatherData (Tuple4<Double, Double, Double, Integer> v1,
                                          Tuple4<Double, Double, Double, Integer> v2) {
        return new Tuple4(v1._1() + v2._1(),
                v1._2() + v2._2(),
                v1._3() + v2._3(),
                v1._4() + v2._4());
    }

    /*
     * This function converts multiple (key-> value) pairs to one single (key -> value) pair by adding up all
     * the values associated with a single key
     */
    private static Tuple3<Double, Double, Double> avgWeatherDataFunc
            (Tuple4<Double, Double, Double, Integer> t) {
        return new Tuple3<>(t._1()/t._4(), t._2()/t._4(), t._3()/t._4());
    }

    /*
    * This function just adds the weather data values for two tuples. This function is called over and over
    * until each item in the stream (during a windows) has been added.
     */
    private static Tuple2<Tuple2<String, Precipitation>, Tuple4<Double, Double, Double, Integer>> weatherFunc
            (
                    Tuple2<Tuple2<String, Precipitation>, Tuple4<Double, Double, Double, Integer>> t1,
                    Tuple2<Tuple2<String, Precipitation>, Tuple4<Double, Double, Double, Integer>> t2
            ) {
        return new Tuple2<Tuple2<String, Precipitation>, Tuple4<Double, Double, Double, Integer>>
                (new Tuple2(t1._1._1, t1._1._2), addWeatherData(t1._2, t2._2));

    }

    /*
     * This function manipulates the stream of WeatherData objects in order to compute average of all the
     * weather data attributes
     */
    private void aggregateWeatherData(JavaDStream<WeatherData> weatherDataStream) {
        //Convert (WeatherData -> <(sensorId, Precipitation) -> (temperature, windspeed, visibility, keycount)>)
        //Keycount is maintained so that I can get the denominator for calculating average.
        //For this I just add up all the 1's in the fourth field of the tuple
        JavaPairDStream<Tuple2<String, Precipitation>, Tuple4<Double, Double, Double, Integer>>
                weatherDataPair= weatherDataStream.mapToPair((t) ->
                new Tuple2(new Tuple2(t.getSensorId(), t.getPreciptation()),
                        new Tuple4<>(
                                t.getTemperature(),
                                t.getWindSpeed(),
                                t.getVisibility(),
                                1
                                )));

        //Convert <sensorId, Precipitation> -> <temperature, windspeed, visibility, keycount>
        //TO
        //<sensorId, Precipitation> -> <sum of temperatures, sum of windspeeds, sum of visiblity, sum of keycounts>
        JavaDStream<Tuple2<Tuple2<String, Precipitation>, Tuple4<Double, Double, Double, Integer>>>
                weathersumStream = weatherDataPair.reduceByWindow(
                        (x,y) -> weatherFunc(x,y),
                        Durations.seconds(15),
                        Durations.seconds(15));

        //Convert <<sensorId, Precipitation> -> <sum of temperatures, sum of windspeeds, sum of visiblity, sum of keycounts>
        //TO
        //<sensorId, Precipitation> -> <sum of temperatures/sum of keycount,
        // sum of windspeeds / sum of keycount,
        // sum of visiblity / sum of keycount>
        JavaDStream<Tuple2<Tuple2<String, Precipitation>, Tuple3<Double, Double, Double>>> avgWeatherStream =
                weathersumStream
                .map(x -> new Tuple2(x._1, avgWeatherDataFunc(x._2)));

        //Print average weather statistics for each sensorId
        avgWeatherStream.print();

    }

    public void consume() {
        Properties properties = PropertyFileReader.readConsumerPropertyFile();
        Properties prodProperties = PropertyFileReader.readProducerPropertyFile();
        SparkConf sparkConf = new SparkConf()
                .setAppName(properties.getProperty("com.iot.app.spark.app.name"))
                .setMaster(properties.getProperty("com.iot.app.spark.master"))
                .set("spark.streaming.concurrentJobs", "4");

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

        JavaDStream<WeatherData> nonFilteredWeatherDataStream = weatherKafkaStream.map(tuple -> tuple._2());
        aggregateWeatherData(nonFilteredWeatherDataStream);
        //produceCarNotifications(nonFilteredDataStream, prodProperties);
        //nonFilteredDataStream.print();

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
                        String topic = properties.getProperty("com.iot.app.kafka.topic");
                        Random random = new Random();

                        CarNotificationData carNotificationData = new CarNotificationData(carData.getCarId(),
                                carData.getSpeed());

                        KeyedMessage<String, CarNotificationData> keyedMessage = new KeyedMessage<>(topic,
                                carNotificationData);
                        producer.send(keyedMessage);
                    }
                }
        );
    }
}
