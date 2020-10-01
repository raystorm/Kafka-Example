package com.github.raystorm.Kafkaexample.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.ABSwitchCluster;
import org.springframework.kafka.core.KafkaResourceFactory;
import org.springframework.kafka.core.KafkaTemplate;


/**
 *  Post Processing Bean to ensure {@link KafkaSwitchCluster} is wired in properly
 *  @author tbus8
 */
@Slf4j
@Configuration
@Profile({"kafka-switch-test", "kafka-lle", "kafka-prod"})
public class KafkaPostProcessor implements BeanPostProcessor
{
   @Autowired
   ABSwitchCluster KafkaSwitchCluster;

   @Autowired
   KafkaProducerErrorHandler errorHandler;

   @Override
   public Object postProcessBeforeInitialization(Object bean, String beanName)
          throws BeansException
   {
      return bean;
   }

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
