package com.github.raystorm.Kafkaexample.config;

import java.util.*;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.BrokerNotAvailableException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.TimeoutException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.*;
import org.springframework.kafka.event.ListenerContainerIdleEvent;
import org.springframework.kafka.event.NonResponsiveConsumerEvent;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.context.WebApplicationContext;


/**
 *  Error Handler to Detect Broker down on Send.
 */
@Slf4j
@Component
@Profile({"kafka-switch-test", "kafka-lle", "kafka-prod"})
public class KafkaProducerErrorHandler implements ProducerListener<Object, Object>
{
   private  static final String error_UID = "da2ba106-d343-43f3-84a9-37f8e769a3b5";

   private static final String event_UID = "17111a0d-34f5-4d7f-82ff-ad7e85a41f03";

   private static final String timer_UID = "c8303e58-9d07-418f-b9bd-cf35e00f8b17";

   private static Date lastSwitchTime = new Date(0); //initialize to epoch

   @Autowired
   KafkaAdmin kafkaAdmin;

   @Autowired
   KafkaSwitchCluster kafkaSwitchCluster;

   @Autowired
   WebApplicationContext context;

   @Autowired
   KafkaListenerEndpointRegistry registry;

   @Value("${spring.kafka.properties.primary-servers}")
   String primaryServersList;

   @Value("${spring.kafka.properties.secondary-servers}")
   String secondaryServersList;

   /**
    *  Unable to use {@link Autowired} due to circular dependency
    *  with {@link KafkaPostProcessor}
    *  @return
    */
   public DefaultKafkaProducerFactory getDefaultKafkaProducerFactory()
   { return context.getBean(DefaultKafkaProducerFactory.class); }

   /**
    *  Unable to use {@link Autowired} due to circular dependency
    *  with {@link KafkaPostProcessor}
    *  @return
    */
   public DefaultKafkaConsumerFactory getDefaultKafkaConsumerFactory()
   { return context.getBean(DefaultKafkaConsumerFactory.class); }

   public AdminClient getAdminClient(String bootstrapServers)
   {
      Map<String, Object> config =
         new HashMap<>(kafkaAdmin.getConfigurationProperties());

      config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

      return KafkaAdminClient.create(config);
   }

   public AdminClient primaryAdminClient()
   {
      Map<String, Object> config =
              new HashMap<>(kafkaAdmin.getConfigurationProperties());

      config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, primaryServersList);

      return KafkaAdminClient.create(config);
   }

   public AdminClient secondaryAdminClient()
   {
      Map<String, Object> config = kafkaAdmin.getConfigurationProperties();

      config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, secondaryServersList);

      return KafkaAdminClient.create(config);
   }

   /**
    *  Helper Method to check if the Brokers are really down.
    *  Inspired by: https://stackoverflow.com/a/49852168/659354
    *  @return
    */
   public boolean areBrokersUp()
   {
      String boostrapservers = kafkaSwitchCluster.isPrimary()?
                               primaryServersList : secondaryServersList;
      return areBrokersUp(boostrapservers);
   }


   public boolean areBrokersUp(String boostrapservers)
   {
      //TODO: make this whole method automic
      // - don't send too many other messages while checking
      /*
      boolean switched = false;
      if ( isPrimary != kafkaSwitchCluster.isPrimary() )
      {
         //if (isPrimary) { kafkaSwitchCluster.primary(); }
         //else { kafkaSwitchCluster.secondary(); }
         switchCluster();
         switched = true;
      }
      */

      //TODO: create an Admin Client that doesn't impact everything else
      try ( AdminClient client = getAdminClient(boostrapservers) )
      {
         //original gets topics
         ListTopicsResult topics = client.listTopics();
         Set<String> names = topics.names().get();
         if (names.isEmpty()) { return false; }

         return true;
      }
      //assume down if caught
      catch (InterruptedException | ExecutionException e) { return false; }
      /*
      finally
      {
         if (switched) //switch back.
         {
            //if (isPrimary) { kafkaSwitchCluster.secondary(); }
            //else { kafkaSwitchCluster.primary(); }
            switchCluster();
         }
      }
      */
   }

   /** Back-End Method to Actually Switch between the clusters */
   private void switchCluster()
   {
      registry.stop();
      if (kafkaSwitchCluster.isPrimary()) { kafkaSwitchCluster.secondary(); }
      else { kafkaSwitchCluster.primary(); }
      registry.start();
   }

   /** Back-End Method for Handling the Consumer Event based switching */
   private void switchClusterEvent(String UID)
   { switchClusterEvent(UID, false); }

   /**
    *  Back-End Method for Handling the Consumer Event based switching
    *  @param UID  UID Number for logging
    *  @param force boolean - indicates if the switch should be forced (true),
    *                         or only run if the brokers are down (false)
    */
   private void switchClusterEvent(String UID, boolean force)
   {
      log.info("running for switchClusterEvent " + UID);
      synchronized (lastSwitchTime)
      {
         //if Brokers are up, do nothing.
         if ( areBrokersUp() ) { return; }

         Calendar switchable = Calendar.getInstance();
         switchable.setTime(lastSwitchTime);
         //see if last switch was within 30 seconds
         switchable.add(Calendar.SECOND, 30);
         //         switchable.add(Calendar.MINUTE, 2);
         //bail if enough time hasn't elapsed since the last switch
         if (switchable.after(Calendar.getInstance())) { return; }

         //toggle to opposite of current setting.
         log.warn(UID + " -- Switching Cluster because the container is down.");

         switchCluster();

         //reset the switch time
         lastSwitchTime.setTime(new Date().getTime());
      }
   }

   @Override
   public void onError(ProducerRecord<Object, Object> producerRecord,
                       Exception exception)
   {
      if ( exception instanceof TimeoutException
        || exception instanceof BrokerNotAvailableException
        || exception instanceof DisconnectException )
      {  //issues connecting to Broker.
         log.warn(error_UID + " -- Error connecting to Broker, checking broker status.");

         //if Brokers are up, do nothing.
         if ( !areBrokersUp() ) { return; }

         log.warn(error_UID + " -- Broker confirmed DOWN, failing back to other cluster.");
         switchCluster();
      }
   }

   /**
    *  Event Handler to Respond to Broker Down (long time without message)
    *  @param event
    */
   @EventListener()
   public void eventHandler(final ListenerContainerIdleEvent event)
   {
      log.warn("Kafka Cluster Container Idle Event Handled.");
      log.info(event.toString());
      switchClusterEvent(event_UID);
   }

   /**
    *  Event Handler to Respond to Broker Down (long time without message within poll)
    *  @param event
    */
   @EventListener()
   public void eventHandler(final NonResponsiveConsumerEvent event)
   {
      log.warn("Kafka Cluster NonResponsive Event Handled.");
      switchClusterEvent(event_UID);
   }

   /**
    *  Runs on a fixed Schedule to Ensure Kafka is Connected to Primary if up.
    */
   @Scheduled(fixedRateString = "${spring.kafka.properties.switchBackTimer}")
   public void primarySwitch()
   {
      if ( log.isDebugEnabled() )
      {
         log.debug("firing Scheduled primarySwitch: " + timer_UID);
         log.debug("Scheduled primarySwitch isPrimary: "
                  + kafkaSwitchCluster.isPrimary());
         log.debug("Scheduled primarySwitch areBrokersUp: "
                  + areBrokersUp(primaryServersList));
      }

      //skip if we are already in Primary, or the primary brokers are down.
      if ( kafkaSwitchCluster.isPrimary() || !areBrokersUp(primaryServersList) )
      { return; }

      log.debug("Switching cluster during Scheduled primarySwitch: " + timer_UID);

      //still here, assume, on Secondary, and Primary is up.
      switchClusterEvent(timer_UID, true);
   }

}
