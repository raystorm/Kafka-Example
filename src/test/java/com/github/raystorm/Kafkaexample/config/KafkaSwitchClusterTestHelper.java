package com.github.raystorm.Kafkaexample.config;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.ABSwitchCluster;
import org.springframework.stereotype.Service;


/**
 *  Provides Primary / Secondary Cluster Configuration Toggle for Kafka<br />
 *  Test Wrapper for Dependency Mgmt to ensure brokers are available.
 *    
 *  @see KafkaSwitchCluster
 */
@Service
@Profile({"kafka-switch-test"})
@DependsOn(value = {"embeddedKafka", "secondaryBroker"})
public class KafkaSwitchClusterTestHelper extends KafkaSwitchCluster
{
   private static final Logger log = 
           LoggerFactory.getLogger(KafkaPostProcessor.class);
   
   public KafkaSwitchClusterTestHelper(@Value("${spring.kafka.properties.primary-servers}")
                                       String Primary,
                                       @Value("${spring.kafka.properties.secondary-servers}")
                                       String Secondary)
   { super(Primary, Secondary); }
}
