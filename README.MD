# Distributed Systems Project

## Goal
Demonstrating the usage of Spark and Messaging systems like Kafka to analyze IOT data patterns and provide real-time analytics and alerting

## Data Sources
* Vehicle Data from cars
* Weather data from sensors

## Use Cases
* Push hyper local weather updates to cars in sensor's range
* Alert cars about hazardous weather
* Track and aggregate car activity in order to help insurance companies make the correct decision in event of an accident

## Hazardous Weather Conditions
* Blizzard conditions
* Wind Advisory
* Fog Advisory

## Rules for Hazardous Weather Detection
* Blizzards - windSpeed > 35 mph and Visibility < quarter mile and Precipitation = snow and timeElapsed > 2 hrs
* Wind Advisory - windSpeed > 40 mph and timeElapsed > 2 hrs
* Fog - Visibility < 2 miles and timeElapsed > 1 hrs

## Architecture Diagram

## Communication Protocol Between Car and Sensor
* The mode of communication will be <b>TCP</b> as the connection has to stay active to send updates back to car
* The car has the <b>list of known hosts</b> it can connect to once it is started
* The sensor has a limited connection pool i.e at any instance the sensor can only be connected to limited number of cars

## Communication Protocol Between Sensor and Kafka Broker

## Communication Between Between Sensor and Spark

## Communication Between Kafka and Spark

## Data Model
### Car Data
```
public class CarData implements Serializable {
    private String carId;
    private String latitude;
    private String longitude;
    private Long timestamp;
    private double speed;
    private double fuelLevel;
}
```
### Weather Data
```
public class WeatherData implements Serializable {
    private String sensorId;
    private double latitude;
    private double longitude;
    private Long timestamp;
    private double temperature;
    private Precipitation preciptation;
    private double windSpeed;
    private double visibility;
}

public enum Precipitation {
    RAIN,
    SNOW,
    HAIL,
    NORMAL
}
```

## Functionality related to vehicles

* Create vehicle topic on broker
* Define vehicle data class
* Write a vehicle data generator as kafka prod
* Create a spark stream to listen for vehicle_topic
* Implement serializer_desrializer for vehicle data
* Print the contents of spark stream to check vehicle data

## Functionality related to weather

* Weather data class
* Weather producer class
* Spark stream for weather producer

## Establishing communication between car and sensor

* Making sensor a receiver
* Publish weather data to car
* Publish hazards data to car

## TODO
* The average weather data has to be leveraged publish the alert to the weather alerts topic
* The sensor has to act on the weather alert and notify the car
* Establish connection between car and sensor
* Batch car statistics and weather statistics by storing on Cassandra
* Run analytics on top of cassandra through RESTFUL service and webUI
* Think about using the same producer code on a rasberryPi and ODroid device instead of a computer
* Think of tuning performance and energy requirements for computation happening on the edge device

