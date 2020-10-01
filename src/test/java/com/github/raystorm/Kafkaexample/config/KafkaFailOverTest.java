package com.github.raystorm.Kafkaexample.config;


import com.github.raystorm.Kafkaexample.config.util.KafkaConsumerGroupRandomizer;
import kafka.common.KafkaException;
import kafka.server.KafkaServer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.util.concurrent.ListenableFuture;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;


/**
 *  Tests for Kafka handling cluster Failover and return safely
 */
@Slf4j
@RunWith(SpringRunner.class)
@SpringBootTest()
@ActiveProfiles({"inmemory", "test", "kafka-switch-test"})
@WebAppConfiguration
@DirtiesContext
@EmbeddedKafka(bootstrapServersProperty = "spring.embedded.kafka.brokers_secondary")
public class KafkaFailOverTest
{
   private static final String topic = "com.github.raystorm.test.cluster";

   @ClassRule
   public static EmbeddedKafkaRule embeddedKafkaRule = new EmbeddedKafkaRule(1, true, topic);

   @ClassRule
   public static final EmbeddedKafkaRule embeddedKafkaRule_secondary = new EmbeddedKafkaRule(1, true, topic)
           .brokerProperty("bootstrapServersProperty",
                   "spring.embedded.kafka.brokers_secondary");


   @TestConfiguration
   static class testConfig
   {
      @Bean
      public ProducerFactory<String, String> kafkaProducerFactory(ApplicationContext ctx)
      {
         Map<String, Object> configProps =
                 KafkaTestUtils.producerProps(embeddedKafkaRule_secondary.getEmbeddedKafka());

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

      @Bean
      public KafkaSHProperties kafkaSHProperties(KafkaConsumerGroupRandomizer randomizer)
      {
         return new KafkaSHProperties(topic,
                 "com.github.raystorm.test.cache-cg."+randomizer.generate(),
                 "com.github.raystorm.test.stream-v1.0.1-a",
                 "com.github.raystorm.test.",
                 ".alloc.request",
                 ".alloc.output");
      }
   }

   @Autowired
   KafkaSwitchCluster kafkaSwitchCluster;

   @Autowired
   private EmbeddedKafkaBroker embeddedKafka;

   @Autowired
   private EmbeddedKafkaBroker embeddedKafka_secondary;

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

   private String topic1;

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
      embeddedKafka = embeddedKafkaRule.getEmbeddedKafka();

      startup_primary();

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

      topic1 = properties.getCacheTopic();
      assertThat(topic1, is(topic));

      log.info("Kafka Running, Adding Topic.");

      try { embeddedKafka.addTopics(topic1); }
      catch (KafkaException Ignored) { }

      assertThat(embeddedKafka.getTopics(), hasItem(topic1));

      assertThat(embeddedKafka.getPartitionsPerTopic(), is(2));

      try { embeddedKafka_secondary.addTopics(topic1); }
      catch (KafkaException Ignored) { }

      assertThat(embeddedKafka_secondary.getTopics(), hasItem(topic1));

      assertThat(embeddedKafka_secondary.getPartitionsPerTopic(), is(2));

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
      assertThat(topics.size(),is(1) );
      assertThat(topics, hasItem(topic1) );

      log.info("Topics again.");

      Consumer<String, String> consumer =
              consumerFactory.createConsumer(properties.getCacheConsumptionGroup(),
                                             properties.getEnvironmentPrefix(),
                                             properties.getOutputTopicSuffix());

      log.info("ConsumerFactory built");

      Set<String> ctops = consumer.listTopics().keySet();
      assertThat(ctops, hasItem(topic1));

      log.info("ConsumerFactory validated");

      //create/reset a thread safe queue to store the received message
      records = new LinkedBlockingQueue<>();
   }

   public void send(String cm) { send(cm, false); }

   public void send(String cm, boolean isRetry)
   {
      ListenableFuture<SendResult<String, String>> sent =
              kafkaTemplate.send(topic1, UUID.randomUUID().toString(), cm);
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
              kafkaTemplate.send(topic1, UUID.randomUUID().toString(), cm);
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
      //registry.stop();
      kafkaSwitchCluster.Rochelle = embeddedKafka.getBrokersAsString();
      for(KafkaServer ks : embeddedKafka.getKafkaServers()) { ks.startup(); }
      //registry.start();
   }

   public void shutdownBroker_primary()
   {
      for(KafkaServer ks : embeddedKafka.getKafkaServers())
      { ks.shutdown(); }
      for(KafkaServer ks : embeddedKafka.getKafkaServers())
      { ks.awaitShutdown(); }
   }

   public void shutdownBroker_secondary()
   {
      for(KafkaServer ks : embeddedKafka_secondary.getKafkaServers())
      { ks.shutdown(); }
      for(KafkaServer ks : embeddedKafka_secondary.getKafkaServers())
      { ks.awaitShutdown(); }
   }

   @After
   public void tearDown() throws Exception {}

   public String getBootStrapServersList()
   {
      Map<String, Object> configs = kafkaTemplate.getProducerFactory()
              .getConfigurationProperties();

      return configs.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG).toString();
   }

   @Test
   public void send_failover() throws Exception
   {
      //Get ABSwitchCluster to check failover details
      KafkaSwitchCluster ktSwitch = (KafkaSwitchCluster)
              ((BootStrapExposerProducerFactory)
                       kafkaTemplate.getProducerFactory()).getBootStrapSupplier();

      assertThat(ktSwitch,             notNullValue());
      assertThat(ktSwitch.get(),       is(Primary));
      assertThat(ktSwitch.isPrimary(), is(true));

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
               //for (int i = 0; i <= timeOut && ktSwitch.isPrimary(); ++i)
               while ( ktSwitch.isPrimary() )
               {  //poll for cluster switch
                  lock.wait(Duration.ofSeconds(15).toMillis());
               }
            }
         }
         catch (InterruptedException IGNORE)
         { fail("Unable to wait for cluster switch. " + IGNORE.getMessage()); }
      }

      //Confirm Failover has happened
      assertThat(ktSwitch.isPrimary(),      is(false));
      assertThat(ktSwitch.get(),            is(Secondary));
      assertThat(getBootStrapServersList(), is(Secondary));

      assertThat(kafkaSwitchCluster.get(),       is(Secondary));
      assertThat(kafkaSwitchCluster.isPrimary(), is(false));

      //Send a message on backup server
      String message = "Test Failover";
      send(message);

      String msg = records.poll(30, TimeUnit.SECONDS);
      assertThat(msg, notNullValue());
      assertThat(msg, is(message));

      log.info("Test is finished");

      //reset for next test
      EmbeddedKafkaRule newRule = new EmbeddedKafkaRule(1, true, topic)
              .brokerProperty("bootstrapServersProperty",
                              "spring.embedded.kafka.brokers_secondary");

      log.info("Rebuild Kafka.");
      registry.stop();
      embeddedKafka = newRule.getEmbeddedKafka();
      ktSwitch.Rochelle = embeddedKafka.getBrokersAsString();
      embeddedKafkaRule = newRule;

      /*
      for ( KafkaServer ks : newRule.getEmbeddedKafka().getKafkaServers() )
      { ks.startup(); }
       */

      startup_primary();
   }
}