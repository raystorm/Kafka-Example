package com.github.raystorm.Kafkaexample.config;


import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import kafka.common.KafkaException;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
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
import org.springframework.context.annotation.Primary;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.serializer.JsonDeserializer;
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
 *  Tests for Kafka handling cluster Failover and return safely
 */
@RunWith(SpringRunner.class)
@SpringBootTest()
@ActiveProfiles({"inmemory", "test", "kafka-switch-test", "kafka-test"})
@WebAppConfiguration
@DirtiesContext
@EnableScheduling
@EmbeddedKafka(topics = "com.github.raystorm.test.cluster",
               bootstrapServersProperty = "spring.kafka.properties.primary-servers")
public class KafkaSwitchBackTest
{
   private static final Logger log = 
         LoggerFactory.getLogger(KafkaSwitchBackTest.class);
   
   private static final String topic = "com.github.raystorm.test.cluster";

   @TestConfiguration
   @EnableScheduling
   static class testConfig
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
         Map<String, Object> configProps = KafkaTestUtils.producerProps(broker);

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
      @Primary
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
   KafkaSwitchCluster kafkaSwitchCluster;

   @Autowired
   private KafkaProducerErrorHandler kafkaProducerErrorHandler;

   @Autowired()
   ApplicationContext applicationContext;

   @Autowired
   KafkaTemplate<String, String> kafkaTemplate;

   @Autowired
   KafkaListenerEndpointRegistry registry;

   @Autowired
   KafkaSHProperties properties;

   String am = "Test Kafka Message";

   @Value("${spring.kafka.properties.primary-servers}")
   public String Primary;

   @Value("${spring.kafka.properties.secondary-servers}")
   public String Secondary;

   /* read sent messages  */
   static BlockingQueue<String> records;

   Object lock = new Object();

   @KafkaListener(topics = { topic },
                  groupId = "com.github.raystorm.Kafkaexample.config.KafkaSwitchClusterTest")
   public void listen(String msg)
   {
      System.out.println("Kafka Listener: " + msg);
      if ( !records.add(msg) ) { fail("Unable to store Kafka Message"); }
      System.out.println("Kafka Contents: " + records.toString());
   }

   /*
    *  Broker Statuses
    */

   byte NotRunning                    = (byte)0;
   byte Starting                      = (byte)1;
   byte RecoveringFromUncleanShutdown = (byte)2;
   byte RunningAsBroker               = (byte)3;
   byte PendingControlledShutdown     = (byte)6;
   byte BrokerShuttingDown            = (byte)7;

   int timeOut = 20; //15 * 20 = 300  = 5 minutes

   @Before
   public void setUp() throws Exception
   {
      synchronized (lock) //pause for startup
      { lock.wait(Duration.ofSeconds(3).toMillis()); }

      String brokers = embeddedKafka.getBrokersAsString();

      if ( !kafkaProducerErrorHandler.areBrokersUp(brokers) )
      {
         synchronized (lock)
         {
            for ( int i=0;
                  i <= 10 && !kafkaProducerErrorHandler.areBrokersUp(brokers)
                  && registry.isRunning();
                  ++i )
            { lock.wait(Duration.ofSeconds(1).toMillis()); }
         }
      }

      assertThat(kafkaProducerErrorHandler.areBrokersUp(brokers), is(true));

      log.info("Kafka Running, Adding Topic.");

      assertThat(embeddedKafka.getTopics(), hasItem(topic));
      assertThat(embeddedKafka.getPartitionsPerTopic(), is(2));

      try { secondaryBroker.addTopics(topic); }
      catch (KafkaException ignored) { }

      assertThat(secondaryBroker.getTopics(), hasItem(topic));
      assertThat(secondaryBroker.getPartitionsPerTopic(), is(2));

      log.info("Topics validated.");

      // set up the Kafka consumer properties
      Map<String, Object> consumerProperties =
         KafkaTestUtils.consumerProps(properties.getCacheConsumptionGroup(),
                                      "false", embeddedKafka);

      // create a Kafka consumer factory
      DefaultKafkaConsumerFactory<String, String> consumerFactory =
              new DefaultKafkaConsumerFactory<>(consumerProperties,
                                                new StringDeserializer(),
                                                new JsonDeserializer<String>());

      log.info("DefaultKafkaConsumerFactory built");

      Set<String> topics = embeddedKafka.getTopics();
      assertThat(topics.size(), is(1) );
      assertThat(topics,        hasItem(topic));

      log.info("Topics again.");

      Consumer<String, String> consumer =
              consumerFactory.createConsumer(properties.getCacheConsumptionGroup(),
                                             properties.getEnvironmentPrefix(),
                                             properties.getOutputTopicSuffix());

      log.info("ConsumerFactory built");

      Set<String> ctops = consumer.listTopics().keySet();
      assertThat(ctops, hasItem(topic));

      log.info("ConsumerFactory validated");

      //create/reset a thread safe queue to store the received message
      records = new LinkedBlockingQueue<>();
   }

   public void send(String cm) { send(cm, false); }

   public void send(String cm, boolean isRetry)
   {
      ListenableFuture<SendResult<String, String>> sent =
              kafkaTemplate.send(topic, UUID.randomUUID().toString(), cm);
      kafkaTemplate.flush();

      System.out.println("Sending message ["+cm+"] to: " + getBootStrapServersList());

      try
      {
         SendResult<String, String> result = sent.get(10, TimeUnit.SECONDS);
         System.out.println("Cache Message Sent successful.");
      }
      catch(Exception ex) { fail("Error: sending message: " + ex.getMessage()); }
   }

   public void send_expectFailure(String cm)
   {
      ListenableFuture<SendResult<String, String>> sent =
              kafkaTemplate.send(topic, UUID.randomUUID().toString(), cm);
      kafkaTemplate.flush();

      try
      {
         SendResult<String, String> result = sent.get(10, TimeUnit.SECONDS);
         System.out.println("Cache Message Sent successful.");
      }
      catch(Exception ex)
      { log.warn("Encountered expected Error Sending Kafka Message.", ex); }
   }

   public void startup_primary()
   {
      embeddedKafka.afterPropertiesSet();
      if ( log.isDebugEnabled() )
      {
         log.debug("Does KS = primary? "
                   + kafkaSwitchCluster.Primary + " == "
                   + embeddedKafka.getBrokersAsString());
      }
   }

   public void shutdownBroker_primary() { embeddedKafka.destroy(); }

   public void shutdownBroker_secondary() { secondaryBroker.destroy(); }

   @After
   public void tearDown() throws Exception {}

   public String getBootStrapServersList()
   {
      Map<String, Object> configs = kafkaTemplate.getProducerFactory()
              .getConfigurationProperties();

      return configs.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG).toString();
   }

   @Test
   public void send_switchback_direct() throws Exception
   {
      assertThat(kafkaTemplate.getProducerFactory(),
                 instanceOf(BootStrapExposerProducerFactory.class));

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
      shutdownBroker_primary();
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

      String msg = records.poll(10, TimeUnit.SECONDS);
      assertThat(msg, notNullValue());
      assertThat(msg, is(message));

      log.info("Rebuild Kafka.");
      startup_primary();

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

      //This directly fires, separate method tests scheduled
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

      assertThat(brokers,              anyOf(is(Primary), is(Secondary))); //port didn't change
      assertThat(brokers,              is(Primary)); //is primary
      assertThat(kafkaSwitchCluster.isPrimary(), is(true));
      //assertThat(ktSwitch.isPrimary(), is(true));
      assertThat(ktSwitch.get(),       is(brokers));

      assertThat(kafkaProducerErrorHandler.areBrokersUp(brokers),  is(true));
      assertThat(kafkaProducerErrorHandler.areBrokersUp(Primary), is(true));

      assertThat(ktSwitch.isPrimary(), is(true));
      //assertThat(ktSwitch.get(),       not(anyOf(is(Hudson), is(Rochelle))));
      assertThat(ktSwitch.get(),       is(embeddedKafka.getBrokersAsString()));

      //Send a message on backup server
      message = "Test newPrimary";
      send(message);

      msg = records.poll(10, TimeUnit.SECONDS);
      assertThat(msg, notNullValue());
      assertThat(msg, is(message));

      log.info("Test is finished");
   }

   @Test
   public void send_switchback_scheduled() throws Exception
   {
      assertThat(kafkaTemplate.getProducerFactory(),
                 instanceOf(BootStrapExposerProducerFactory.class));

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
      shutdownBroker_primary();
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

      String msg = records.poll(10, TimeUnit.SECONDS);
      assertThat(msg, notNullValue());
      assertThat(msg, is(message));

      log.info("Rebuild Kafka.");
      startup_primary();

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

      //test Scheduled Fire
      //kafkaProducerErrorHandler.primarySwitch();

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

      assertThat(brokers,              anyOf(is(Primary), is(Secondary))); //port didn't change
      assertThat(brokers,              is(Primary)); //is primary
      assertThat(kafkaSwitchCluster.isPrimary(), is(true));
      //assertThat(ktSwitch.isPrimary(), is(true));
      assertThat(ktSwitch.get(),       is(brokers));

      assertThat(kafkaProducerErrorHandler.areBrokersUp(brokers),  is(true));
      assertThat(kafkaProducerErrorHandler.areBrokersUp(Primary), is(true));

      assertThat(ktSwitch.isPrimary(), is(true));
      //assertThat(ktSwitch.get(),       not(anyOf(is(Hudson), is(Rochelle))));
      assertThat(ktSwitch.get(),       is(embeddedKafka.getBrokersAsString()));

      //Send a message on backup server
      message = "Test newPrimary";
      send(message);

      msg = records.poll(10, TimeUnit.SECONDS);
      assertThat(msg, notNullValue());
      assertThat(msg, is(message));

      log.info("Test is finished");
   }
}