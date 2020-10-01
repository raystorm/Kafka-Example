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
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.core.BrokerAddress;
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
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;


/**
 *  Tests for Publishing Allocation Results to Kafka
 */
@Slf4j
@RunWith(SpringRunner.class)
@SpringBootTest()
@ActiveProfiles({"inmemory", "test", "kafka-switch-test"})
@WebAppConfiguration
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@EmbeddedKafka(bootstrapServersProperty = "spring.embedded.kafka.brokers_secondary")
public class KafkaSwitchClusterTest
{
   private static final String topic = "com.github.raystorm.test.cluster";

   @ClassRule
   public static final EmbeddedKafkaRule embeddedKafkaRule =
           new EmbeddedKafkaRule(1, true, topic);

   @ClassRule
   public static final EmbeddedKafkaRule embeddedKafkaRule_secondary =
           new EmbeddedKafkaRule(1, true, topic)
                   .brokerProperty("bootstrapServersProperty",
                           "spring.embedded.kafka.brokers_secondary");

   @TestConfiguration
   static class testConfig
   {
      @Bean
      //@Primary
      public ProducerFactory<String, String> kafkaProducerFactory(ApplicationContext ctx)
      {
         Map<String, Object> configProps =
                 KafkaTestUtils.producerProps(embeddedKafkaRule.getEmbeddedKafka());

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
   KafkaListenerEndpointRegistry endpointRegistry;

   @Autowired
   DefaultKafkaProducerFactory producerFactory;


   @Autowired
   KafkaSwitchCluster kafkaSwitchCluster;

   //@Autowired
   private EmbeddedKafkaBroker embeddedKafka;

   @Autowired
   private EmbeddedKafkaBroker embeddedKafka_secondary;

   @Autowired
   private KafkaProducerErrorHandler kafkaProducerErrorHandler;

   @Autowired()
   ApplicationContext applicationContext;

   //@SpyBean
   @Autowired
   KafkaTemplate<String, String> kafkaTemplate;

   @Autowired
   KafkaSHProperties properties;

   String am = "Test Kafka Message";

   @Value("${spring.kafka.properties.primary-servers}")
   public String Rochelle;

   @Value("${spring.kafka.properties.secondary-servers}")
   public String Hudson;

   /* read sent messages  */
   static BlockingQueue<String> records; //= new LinkedBlockingQueue<>();

   private String topic1;

   @KafkaListener(topics = { topic },
           groupId = "com.github.raystorm.configuration.config.KafkaSwitchClusterTest")
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

   @Before
   public void setUp() throws Exception
   {
      //create/reset a thread safe queue to store the received message
      records = new LinkedBlockingQueue<>();

      embeddedKafka = embeddedKafkaRule.getEmbeddedKafka();

      assertThat(kafkaProducerErrorHandler.areBrokersUp(), is(true));

      topic1 = properties.getCacheTopic();
      assertThat(topic1, is(topic));

      try { embeddedKafka.addTopics(topic1); }
      catch (KafkaException Ignored) { }

      assertThat(embeddedKafka.getTopics(), hasItem(topic1));

      assertThat(embeddedKafka.getPartitionsPerTopic(), is(2));

      //Dumps Embedded Kafka configs to logs
      System.out.println("embeddedKafka Config: ");
      Map<Object, Object> kprops = (Map)
              embeddedKafka.getKafkaServer(0).config().props();
      for(Map.Entry<Object, Object> prop : kprops.entrySet())
      {
         System.out.println( "   " + prop.getKey().toString()
                 + " = " + prop.getValue().toString());
      }
      for(BrokerAddress ba : embeddedKafka.getBrokerAddresses())
      { System.out.println("   BrokerAddress = " + ba.toString()); }

      DefaultKafkaProducerFactory dpf = (DefaultKafkaProducerFactory)
              kafkaTemplate.getProducerFactory();
      //Dumps Kafka Template configs to logs
      System.out.println("KafkaTemplate Config: ");
      Map<String, Object> props = dpf.getConfigurationProperties();
      for(Map.Entry<String, Object> prop : props.entrySet())
      {
         System.out.println( "   " + prop.getKey()
                 + " = " + prop.getValue().toString());
      }

      //ensure tests start with Primary
      endpointRegistry.stop();
      kafkaSwitchCluster.primary();
      endpointRegistry.start();

      synchronized (lock)
      {
         while (!endpointRegistry.isRunning())
         { lock.wait(Duration.ofMillis(10).toMillis()); }
      }

      //wait for full init, post primary force
      synchronized(lock)
      { lock.wait(Duration.ofSeconds(1).toMillis()); }

      // set up the Kafka consumer properties
      Map<String, Object> consumerProperties =
              KafkaTestUtils.consumerProps(properties.getCacheConsumptionGroup(),
                      "false", embeddedKafka);

      // create a Kafka consumer factory
      DefaultKafkaConsumerFactory<String, String> consumerFactory =
              new DefaultKafkaConsumerFactory<>(consumerProperties,
                      new StringDeserializer(),
                      new JsonDeserializer<String>());

      // set the topic that needs to be consumed
      ContainerProperties containerProperties = new ContainerProperties(topic1);

      Set<String> topics = embeddedKafka.getTopics();
      assertThat(topics.size(),is(1) );
      assertThat(topics,hasItem(topic1) );

      Consumer<String, String> consumer =
              consumerFactory.createConsumer(properties.getCacheConsumptionGroup(),
                      properties.getEnvironmentPrefix(),
                      properties.getOutputTopicSuffix());

      Set<String> ctops = consumer.listTopics().keySet();
      assertThat(ctops, hasItem(topic1));

//      send(am);
//
//      //get the first one
//      String msg = records.poll(30, TimeUnit.SECONDS);
//      assertThat(msg, notNullValue());
//      assertThat(msg, is(am));

      synchronized(lock)
      { lock.wait(Duration.ofSeconds(10).toMillis()); }

      //reset after setup testing.
      records = new LinkedBlockingDeque<>();
   }

   public void send(String cm) { send(cm, false); }

   public void send(String cm, boolean isRetry)
   {
      ListenableFuture<SendResult<String, String>> sent =
              kafkaTemplate.send(topic1, UUID.randomUUID().toString(), cm);
      kafkaTemplate.flush();

      System.out.println("Sending Cache Message ["+cm+"]");

      try
      {
         SendResult<String, String> result = sent.get(10, TimeUnit.SECONDS);
         System.out.println("Cache Message Sent successful.");
      }
      catch(Exception ex)
      {
         if ( false )
         //if (ex.getCause() instanceof TimeoutException && !isRetry )
         { send(cm, true); }
         else { fail("Error: sending message: " + ex.getMessage()); }
      }
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
      {
         log.warn("Encountered expected Error Sending Kafka Message.", ex);
      }
   }

   public void shutdownBroker_primary()
   {
      for(KafkaServer ks : embeddedKafka.getKafkaServers()) { ks.shutdown(); }
      for(KafkaServer ks : embeddedKafka.getKafkaServers()) { ks.awaitShutdown(); }
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
   public void send_primary() throws Exception
   {
      assertThat(kafkaTemplate.getProducerFactory(),
              instanceOf(BootStrapExposerProducerFactory.class));

      ABSwitchCluster ktSwitch = (ABSwitchCluster)
              ((BootStrapExposerProducerFactory)
                      kafkaTemplate.getProducerFactory()).getBootStrapSupplier();

      assertThat(ktSwitch,             notNullValue());
      assertThat(ktSwitch.get(),       is(Rochelle));
      assertThat(ktSwitch.isPrimary(), is(true));

      assertThat(getBootStrapServersList(), is(Rochelle));

      String message = "Test Primary";
      send(message);

      String msg = records.poll(30, TimeUnit.SECONDS);
      assertThat(msg, notNullValue());
      assertThat(msg, is(message));
   }

   private Object lock = new Object();

   @Test
   public void send_secondary() throws Exception
   {
      //switch to secondary
      endpointRegistry.stop();
      kafkaSwitchCluster.secondary();
      endpointRegistry.start();

      synchronized (lock)
      {
         while (!endpointRegistry.isRunning())
         { lock.wait(Duration.ofSeconds(1).toMillis()); }
      }

      //wait for full initialization once running
      synchronized(lock)
      { lock.wait(Duration.ofSeconds(1).toMillis()); }

      ABSwitchCluster ktSwitch = (ABSwitchCluster)
              ((BootStrapExposerProducerFactory)
                      kafkaTemplate.getProducerFactory()).getBootStrapSupplier();

      assertThat(ktSwitch,                  notNullValue());
      assertThat(ktSwitch.get(),            is(Hudson));
      assertThat(ktSwitch.isPrimary(),      is(false));
      assertThat(getBootStrapServersList(), is(Hudson));

      assertThat(kafkaSwitchCluster.isPrimary(), is(false));
      assertThat(kafkaSwitchCluster.get(), is(Hudson));

      String message = "Test Secondary";
      send(message);

      String msg = records.poll(45, TimeUnit.SECONDS);
      assertThat(msg, notNullValue());
      assertThat(msg, is(message));
   }

}