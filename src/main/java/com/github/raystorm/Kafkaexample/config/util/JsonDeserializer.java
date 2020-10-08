package com.github.raystorm.Kafkaexample.config.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

/**
 * Simple Class for Kafka to Deserialize (resontitute) JSON objects
 * @param <T> Generic - type of input object
 */
public class JsonDeserializer<T> implements Deserializer<T>
{
    private ObjectMapper mapper = new ObjectMapper();

    private Class<T> type;

    public JsonDeserializer() { }

    public JsonDeserializer(Class<T> type) { this.type = type; }


    @Override
    public void configure(Map<String, ?> configs, boolean isKey) { }

    /**
     *  Method to deserialize a JSON string.
     *  Simply wraps {@link ObjectMapper#readValue(String, Class)}
     *  @param topic JSON to deserialize
     *  @param data Class to reconsitute as
     *  @return an reconstituted object
     *  @see ObjectMapper#readValue(String, Class)
     */
    @Override
    public T deserialize(String topic, byte[] data)
    {
       try { return mapper.readValue(new String(data), type); }
       catch (IOException e)
       {
          e.printStackTrace();
          throw new RuntimeException(e);
       }
    }

    @Override
    public void close() { }
}
