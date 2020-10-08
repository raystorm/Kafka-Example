# Kafka-Example

Minimal Example for How-to: 
  1. Failover Kafka between Primary and Secondary clusters
     a. `KafkaProducerErrorHandler`
     b. `KafkaSwitchCluster`
  2. Switch Back to Primary Once it is available again  (on a Timer.)
     a. `KafkaProducerErrorHandler#primarySwitch()`  
        Using `@Scheduled` for the timer.
