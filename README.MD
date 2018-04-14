# Distributed Systems Project

## Rules

* Temperature
* Precipitation - rain, snow, hail
* Wind seped
* Visibility

* Blizzards - Wind speeds > 35 mph; Visibility < querter mile; Precipitation - snow and time elapsed > 3 
* Wind Advisory - Strong winds > 40 mph; Time elapsed > 2 hrs
* Fog - Visibility < 2 miles; Time elapsed > 1

## Functionality related to vehicles

### Car Data class
### Car Kafka producer class
### Spark stream for car producer

* Create vehicle topic on broker
* Define vehicle data class
* Write a vehicle data generator as kafka prod
* Craete a spark stream to listen for vehicle_topic
* Implement serializer_desrializer for vehicle data
* Print the contents of spark stream to check vehicle data

## Functionality related to weather

### Weather data class
### Weather producer class
### Spark stream for weather producer

## Establishing communication between car and sensor

### Making sesnsor a receiver
### Publish weather data to car
### Publish hazards data to car

