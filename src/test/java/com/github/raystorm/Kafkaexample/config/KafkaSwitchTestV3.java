package com.github.raystorm.Kafkaexample.config;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.util.concurrent.ListenableFuture;


/**
 * From Scratch Test Re-write based on:
 * https://stackoverflow.com/a/64157233/659354
 */
@RunWith(SpringRunner.class)
@SpringBootTest()
@ActiveProfiles({"inmemory", "test", "kafka-switch-test", "kafka-test"})
@WebAppConfiguration
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@EmbeddedKafka(topics = "com.github.raystorm.test.cluster.v3",
               bootstrapServersProperty = "spring.kafka.properties.primary-servers")
public class KafkaSwitchTestV3
{
   private static final Logger log = 
           LoggerFactory.getLogger(KafkaPostProcessor.class);
   
   
   private static final String topic = "com.github.raystorm.test.cluster.v3";

   @TestConfiguration
   @EnableScheduling
   public static class Config
   {
      @Bean(name = "secondaryBroker")
      @Order(value = Ordered.HIGHEST_PRECEDENCE)
      EmbeddedKafkaBroker secondaryBroker()
      {
         log.debug("building Secondary Broker.");
         return new EmbeddedKafkaBroker(1, true, topic)
                            .brokerListProperty("spring.embedded.kafka.brokers_secondary");
      }

      @Bean
      public ProducerFactory<String, String> kafkaProducerFactory(ApplicationContext ctx)
      {
         EmbeddedKafkaBroker broker = ctx.getBean("embeddedKafka",
                                                  EmbeddedKafkaBroker.class);
         Map<String, Object> configProps =
                 KafkaTestUtils.producerProps(broker);

         System.out.println("Producer Configs from Broker");
         for(Map.Entry<String, Object> config : configProps.entrySet())
         {
            System.out.println( "   " + config.getKey()
                                + "["+config.getValue().getClass().getName()+"]"
                                + " = " + config.getValue().toString());
         }

         BootStrapExposerProducerFactory<String, String> dpf =
                 new BootStrapExposerProducerFactory<>(configProps,
                                                       new StringSerializer(),
                                                       new JsonSerializer());
         dpf.setApplicationContext(ctx);

         return dpf;
      }

      @Bean
      public KafkaTemplate<String, ?> kafkaTemplate(ApplicationContext ctx)
      {
         System.out.println("Creating Test KafkaTemplate.");
         return new KafkaTemplate<>(kafkaProducerFactory(ctx), true);
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

   @Value("${spring.kafka.properties.primary-servers}")
   public String Primary;

   @Value("${spring.kafka.properties.secondary-servers}")
   public String Secondary;

   private Object lock = new Object();

   int timeOut = 20; //15 * 20 = 300  = 5 minutes


   /* read sent messages  */
   static BlockingQueue<String> records = new LinkedBlockingQueue<>();

   @KafkaListener(topics = { topic },
                  groupId = "com.github.raystorm.Kafkaexample.config.KafkaSwitchClusterTest")
   public void listen(String msg)
   {
      System.out.println("Kafka Listener: " + msg);
      if ( !records.add(msg) ) { fail("Unable to store Kafka Message"); }
      System.out.println("Kafka Contents: " + records.toString());
   }

   public void send(String cm)
   { send(cm, false); }

   public void send(String cm, boolean isRetry)
   {
      ListenableFuture<SendResult<String, String>> sent =
              kafkaTemplate.send(topic, UUID.randomUUID().toString(), cm);
      kafkaTemplate.flush();

      System.out.println("Sending message ["+cm+"] to: " + getBootStrapServersList());

      try
      {
         SendResult<String, String> result = sent.get(10, TimeUnit.SECONDS);
         System.out.println("Cache Message Sent successful. " + result.toString());
      }
      catch(Exception ex) { fail("Error: sending message: " + ex.getMessage()); }
   }


   @Test
   public void restartBroker() throws Exception
   {
      SendResult<String, String> sendResult = kafkaTemplate.send(topic, "foo")
                                                           .get(10, TimeUnit.SECONDS);
      System.out.println("+++" + sendResult.getRecordMetadata());

      HashMap map = new HashMap<>();
      map.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
              this.secondaryBroker.getBrokersAsString());

      KafkaTemplate<String, String> secondTemplate = new KafkaTemplate<>(producerFactory, map);
      sendResult = secondTemplate.send(topic, "foo")
                                 .get(10, TimeUnit.SECONDS);
      System.out.println("+++" + sendResult.getRecordMetadata());

      //shutdown
      this.embeddedKafka.destroy();
      this.secondaryBroker.destroy();

      // restart
      try { this.embeddedKafka.afterPropertiesSet(); }
      catch (KafkaException ignored) {}
      try { this.secondaryBroker.afterPropertiesSet(); }
      catch (KafkaException ignored) {}

      sendResult = kafkaTemplate.send(topic, "bar")
                                .get(10, TimeUnit.SECONDS);
      System.out.println("+++" + sendResult.getRecordMetadata());

      sendResult = secondTemplate.send(topic, "bar")
                                 .get(10, TimeUnit.SECONDS);
      System.out.println("+++" + sendResult.getRecordMetadata());
   }


   public String getBootStrapServersList()
   {
      Map<String, Object> configs = kafkaTemplate.getProducerFactory()
                                                 .getConfigurationProperties();

      return configs.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG).toString();
   }

   @Test
   public void send_switchback() throws Exception
   {
      //Get ABSwitchCluster to check failover details
      KafkaSwitchCluster ktSwitch = (KafkaSwitchCluster)
              ((BootStrapExposerProducerFactory)
                       kafkaTemplate.getProducerFactory()).getBootStrapSupplier();

      assertThat(ktSwitch,             notNullValue());
      assertThat(ktSwitch.isPrimary(), is(true));
      assertThat(ktSwitch.get(),       is(Primary));

      assertThat(getBootStrapServersList(), is(Primary));

      log.info("Shutdown Broker to test Failover.");

      //Shutdown Primary Servers to simulate disconnection
      embeddedKafka.destroy();

      //Allow for fail over to happen
      if ( ktSwitch.isPrimary() )
      {
         try
         {
            synchronized (lock)
            {  //pause to give Idle Event a chance to fire
               for (int i = 0; i <= timeOut && ktSwitch.isPrimary(); ++i)
               {  //poll for cluster switch
                  lock.wait(Duration.ofSeconds(15).toMillis());
               }
            }
         }
         catch (InterruptedException IGNORE)
         { fail("Unable to wait for cluster switch. " + IGNORE.getMessage()); }
      }

      //Confirm Failover has happened
      assertThat(ktSwitch.get(),            is(Secondary));
      assertThat(ktSwitch.isPrimary(),      is(false));
      assertThat(getBootStrapServersList(), is(Secondary));

      assertThat(kafkaSwitchCluster.get(),       is(Secondary));
      assertThat(kafkaSwitchCluster.isPrimary(), is(false));

      //Send a message on backup server
      String message = "Test Failover";
      send(message);

      String msg = records.poll(30, TimeUnit.SECONDS);
      assertThat(msg, notNullValue());
      assertThat(msg, is(message));

      log.info("Rebuild/Restart Kafka.");
      embeddedKafka.afterPropertiesSet();
      producerFactory.reset();

      assertThat(embeddedKafka.getBrokersAsString(), is(Primary));
      String brokers = embeddedKafka.getBrokersAsString();

      if ( !kafkaProducerErrorHandler.areBrokersUp(brokers) )
      {
         synchronized (lock)
         {
            for ( int i=0;
                  i <= 15 && !kafkaProducerErrorHandler.areBrokersUp(brokers)
                  && registry.isRunning();
                  ++i )
            { lock.wait(Duration.ofSeconds(1).toMillis()); }
         }
      }

      //Ignores Scheduled fire and calls directly
      kafkaProducerErrorHandler.primarySwitch();

      if ( !kafkaSwitchCluster.isPrimary() )
      {
         try
         {
            synchronized (lock)
            {  //pause to give Idle Event a chance to fire
               for (int i = 0; i <= timeOut && !kafkaSwitchCluster.isPrimary(); ++i)
               {  //poll for cluster switch
                  lock.wait(Duration.ofSeconds(15).toMillis());
               }
            }
         }
         catch (InterruptedException IGNORE)
         { fail("Unable to wait for cluster switch. " + IGNORE.getMessage()); }
      }

      assertThat(brokers,                        anyOf(is(Primary), is(Secondary))); //port didn't change
      assertThat(brokers,                        is(Primary)); //is primary
      assertThat(kafkaSwitchCluster.isPrimary(), is(true));
      assertThat(ktSwitch.isPrimary(),           is(true));
      assertThat(ktSwitch.get(),                 is(brokers));
      assertThat(kafkaSwitchCluster.get(),       is(brokers));

      assertThat(kafkaProducerErrorHandler.areBrokersUp(brokers), is(true));
      assertThat(kafkaProducerErrorHandler.areBrokersUp(Primary), is(true));
      assertThat(registry.isRunning(),           is(true));

      //assertThat(ktSwitch.isPrimary(), is(true));
      //assertThat(ktSwitch.get(),       not(anyOf(is(Hudson), is(Rochelle))));
      assertThat(ktSwitch.get(),       is(embeddedKafka.getBrokersAsString()));

      //Send a message on backup server
      message = "Test newPrimary";
      send(message);

      msg = records.poll(30, TimeUnit.SECONDS);
      assertThat(msg, notNullValue());
      assertThat(msg, is(message));

      log.info("Test is finished");
   }
}
