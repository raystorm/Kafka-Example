package com.github.raystorm.Kafkaexample.config;

import lombok.extern.slf4j.Slf4j;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.ABSwitchCluster;
import org.springframework.stereotype.Service;


/**
 *  Provides Primary / Secondary Cluster Configuration Toggle for Kafka
 */
@Service
@Profile({"kafka-lle","kafka-prod"})
public class KafkaSwitchCluster extends ABSwitchCluster
{
   private static final Logger log = 
		   LoggerFactory.getLogger(KafkaSwitchCluster.class);
   
   public String Primary;

   public String Secondary;

   public KafkaSwitchCluster(@Value("${spring.kafka.properties.primary-servers}")
                             String Primary,
                             @Value("${spring.kafka.properties.secondary-servers}")
                             String Secondary)
   {
      super(Primary, Secondary);

      this.Primary = Primary;
      this.Secondary = Secondary;

      log.info("Starting KafkaSwitchCluster");
   }

   /**
    *  Returns the current Cluster configuration. <br />
    *  <p>
    *     Overriden - for programmatic test location updates.
    *  </p>
    *  @return
    *  @see ABSwitchCluster#get()
    */
   @Override
   public String get() { return isPrimary() ? Primary : Secondary; }
}
