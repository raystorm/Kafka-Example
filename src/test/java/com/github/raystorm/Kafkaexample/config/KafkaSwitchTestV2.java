package com.github.raystorm.Kafkaexample.config;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.concurrent.ListenableFuture;


/**
 * From Scratch Test Re-write based on:
 * https://stackoverflow.com/a/64157233/659354
 */
@RunWith(SpringRunner.class)
@SpringBootTest()
@EmbeddedKafka(topics = "com.github.raystorm.test.cluster.v2",
               bootstrapServersProperty = "spring.kafka.properties.primary-servers")
public class KafkaSwitchTestV2
{
   private static final String topic = "com.github.raystorm.test.cluster.v2";

   @TestConfiguration
   public static class Config
   {
      @Bean
      EmbeddedKafkaBroker secondaryBroker()
      {
         return new EmbeddedKafkaBroker(1, true, topic)
                            .brokerListProperty("spring.kafka.properties.secondary-servers");
      }
   }

   @Autowired
   private EmbeddedKafkaBroker embeddedKafka;

   @Autowired
   private EmbeddedKafkaBroker secondaryBroker;

   @Autowired
   ProducerFactory<String, String> producerFactory;

   @Autowired
   KafkaTemplate<String, String> kafkaTemplate;

   @Autowired
   KafkaListenerEndpointRegistry registry;

   @Autowired
   private KafkaSwitchCluster kafkaSwitchCluster;

   @Autowired
   private KafkaProducerErrorHandler kafkaProducerErrorHandler;

   @Test
   public void restartBroker() throws Exception
   {
      assertThat(kafkaTemplate, notNullValue());

      ListenableFuture<SendResult<String, String>> future =
                      kafkaTemplate.send(topic, "foo");

      SendResult<String, String> sendResult = future.get(10, TimeUnit.SECONDS);
      System.out.println("+++" + sendResult.getRecordMetadata());

      HashMap map = new HashMap<>();
      map.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
              this.secondaryBroker.getBrokersAsString());

      KafkaTemplate<String, String> secondTemplate =
                   new KafkaTemplate<>(producerFactory, map);
      sendResult = secondTemplate.send(topic, "foo")
                                 .get(10, TimeUnit.SECONDS);
      System.out.println("+++" + sendResult.getRecordMetadata());

      //shutdown
      this.embeddedKafka.destroy();
      this.secondaryBroker.destroy();

      // restart
      this.embeddedKafka.afterPropertiesSet();
      this.secondaryBroker.afterPropertiesSet();

      sendResult = kafkaTemplate.send(topic, "bar")
                                .get(10, TimeUnit.SECONDS);
      System.out.println("+++" + sendResult.getRecordMetadata());

      sendResult = secondTemplate.send(topic, "bar")
                                 .get(10, TimeUnit.SECONDS);
      System.out.println("+++" + sendResult.getRecordMetadata());
   }
}
