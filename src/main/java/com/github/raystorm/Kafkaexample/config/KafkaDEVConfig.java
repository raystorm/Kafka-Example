package com.github.raystorm.Kafkaexample.config;

import com.github.raystorm.Kafkaexample.config.util.KafkaConsumerGroupRandomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 *  Configures Kafka Topics for DEV to use to use the names
 */
@Component
@Profile("kafka-dev")
public class KafkaDEVConfig
{
   @Bean
   public KafkaSHProperties kafkaSHProperties(KafkaConsumerGroupRandomizer randomizer)
   {
      return
        new KafkaSHProperties("com.github.raystorm.cache",
                              "com.github.raystorm.cache-cg."+randomizer.generate(),
                              "com.github.raystorm.stream-v1.0.1-a",
                              "com.github.raystorm.",
                              ".alloc.request",
                              ".alloc.output");
   }
}
