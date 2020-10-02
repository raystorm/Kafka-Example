package com.github.raystorm.Kafkaexample.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.ABSwitchCluster;
import org.springframework.stereotype.Service;


/**
 *  Provides Rochelle / Hudson Cluseter Configuration Toggle for Kafka
 */
@Slf4j
//@Component
@Service
@Profile({"kafka-switch-test","kafka-lle","kafka-prod"})
public class KafkaSwitchCluster extends ABSwitchCluster
{
   public String Rochelle;

   public String Hudson;

   public KafkaSwitchCluster(@Value("${spring.kafka.properties.primary-servers}")
                             String Rochelle,
                             @Value("${spring.kafka.properties.secondary-servers}")
                             String Hudson)
   {
      super(Rochelle, Hudson);

      this.Rochelle = Rochelle;
      this.Hudson = Hudson;

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
   public String get() { return isPrimary() ? Rochelle : Hudson; }
}
