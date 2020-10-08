package com.github.raystorm.Kafkaexample.config.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Simple Class for Kafka to be able to Serialize data as JSON
 * @param <T> Generic - type of input object
 */
public class JsonSerializer<T> implements Serializer<T>
{
   private ObjectMapper mapper = new ObjectMapper();

   @Override
   public void configure(Map<String, ?> configs, boolean isKey) { }

   /**
    * Simple wrapper for {@link ObjectMapper#writeValueAsBytes(Object)}
    * @param topic JSON to serialize
    * @param data Object to be Serialized
    * @return byte array serielized representation of the object
    * @see ObjectMapper#writeValueAsBytes(Object)
    */
   @Override
   public byte[] serialize(String topic, T data)
   {
      try { return mapper.writeValueAsBytes(data); }
      catch (JsonProcessingException e)
      {  //TODO: log and duck
         e.printStackTrace();
         throw new RuntimeException(e);
      }
    }

    @Override
    public void close() { }
}
