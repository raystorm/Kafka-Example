package com.github.raystorm.Kafkaexample;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.io.IOException;

/**
 *  Base Application Class for SpringBoot to hook into for its magic.
 */
@SpringBootApplication
@EnableScheduling
public class KafkaExampleApplication
{

    public static void  main (String args[]) throws IOException
    {
       prepare();
       SpringApplication.run(KafkaExampleApplication.class, args);
    }

    /**
     *   Placeholder for any custom init logic.
     *   @throws IOException
     */
    private static void prepare() throws IOException
    {

    }
}
