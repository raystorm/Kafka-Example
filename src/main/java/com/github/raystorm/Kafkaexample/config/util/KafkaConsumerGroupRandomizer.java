package com.github.raystorm.Kafkaexample.config.util;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Slf4j
@Component
public class KafkaConsumerGroupRandomizer
{
    private static int count=0;

    public String generate()
    {
       String value = UUID.randomUUID().toString();
       log.info( "f3a196c6-e35c-417a-9c45-190e8906db9f -- "
               + "random groupId generate, count = {}, value = {}", ++count, value);
       return value;
    }

}
