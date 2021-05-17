package com.github.raystorm.Kafkaexample.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaResourceFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;


/**
 *  Post Processing Bean to ensure {@link KafkaSwitchCluster} is wired in properly
 */
@Configuration
@Profile({"kafka-switch-test", "kafka-lle", "kafka-prod"})
@EnableScheduling
public class KafkaPostProcessor implements BeanPostProcessor
{
   private static final Logger log = 
         LoggerFactory.getLogger(KafkaPostProcessor.class);
   @Autowired
   KafkaSwitchCluster KafkaSwitchCluster;

   @Autowired
   KafkaProducerErrorHandler errorHandler;

   @Override
   public Object postProcessBeforeInitialization(Object bean, String beanName)
          throws BeansException
   { return bean; }

   /**
    *  Hooks into Post Bean Creation to ensure Kafka can Switch
    */
   @Override
   public Object postProcessAfterInitialization(Object bean, String beanName)
          throws BeansException
   {
      if ( bean instanceof KafkaResourceFactory )
      {
         KafkaResourceFactory factory = (KafkaResourceFactory)bean;
         factory.setBootstrapServersSupplier(KafkaSwitchCluster);

         return factory;
      }

      if ( bean instanceof KafkaTemplate )
      {
         KafkaTemplate template = (KafkaTemplate) bean;
         template.setProducerListener(errorHandler);
      }

      return bean;
   }

}
