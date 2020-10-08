package com.github.raystorm.Kafkaexample.config;

import java.util.Map;
import java.util.function.Supplier;

import org.apache.kafka.common.serialization.Serializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;


/** Helper CLass to enable inspecting the BootStrapSupplier to Kafka
 *
 *  @param <K> Key
 *  @param <V> Value
 */
public class BootStrapExposerProducerFactory<K, V>
       extends DefaultKafkaProducerFactory<K, V>
{
   Supplier<String> supplier;

   public BootStrapExposerProducerFactory(Map configs)
   { super(configs); }


   public BootStrapExposerProducerFactory(Map configs,
                                          Serializer<K> keySerializer,
                                          Serializer<V> ValueSerializer)
   { super(configs, keySerializer, ValueSerializer); }

   @Override
   public void setBootstrapServersSupplier(Supplier<String> supplier)
   { super.setBootstrapServersSupplier(supplier); }

   public Supplier<String> getBootStrapSupplier() { return supplier; }
}
