package com.github.raystorm.Kafkaexample;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.io.IOException;

@SpringBootApplication
@EnableScheduling
public class KafkaExampleApplication
{

    public static void  main (String args[]) throws IOException
    {
       prepare();
       SpringApplication.run(KafkaExampleApplication.class, args);
    }

    private static void prepare() throws IOException
    {

    }
}
