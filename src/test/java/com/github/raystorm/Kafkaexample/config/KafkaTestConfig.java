package com.github.raystorm.Kafkaexample.config;

import com.github.raystorm.Kafkaexample.config.util.KafkaConsumerGroupRandomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;


@Component
@Profile("kafka-test")
public class KafkaTestConfig
{
   @Bean
   public KafkaSHProperties kafkaSHProperties(KafkaConsumerGroupRandomizer randomizer)
   {
      return
        new KafkaSHProperties("com.github.raystorm.test.cache",
                              "com.github.raystorm.test.cache-cg."+randomizer.generate(),
                              "com.github.raystorm.test.stream-v1.0.1-a",
                              "com.github.raystorm.test.",
                              ".alloc.request",
                              ".alloc.output");
   }
}
