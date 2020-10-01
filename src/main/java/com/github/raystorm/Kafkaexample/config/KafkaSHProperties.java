package com.github.raystorm.Kafkaexample.config;

import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class KafkaSHProperties
{
    private String cacheTopic;
    private String cacheConsumptionGroup;

    private String streamId;

    private String environmentPrefix;

    private String requestTopicSuffix;
    private String outputTopicSuffix;

    // Lazy Load and Cache the Request/Output Topic

    @Getter(lazy = true)
    private final String requestTopic = constructRequestTopicName();

    @Getter(lazy = true)
    private final String outputTopic = constructOutputTopicName();


    public String constructCacheTopicName()
    { return this.environmentPrefix+cacheTopic; }

    public String constructCacheConsumptionGroupName()
    { return this.environmentPrefix+cacheConsumptionGroup; }

    public String constructRequestTopicName()
    { return (this.environmentPrefix+requestTopicSuffix).replace("..", "."); }

    public String constructClientRequestTopicName(String client)
    { return this.environmentPrefix+client.toLowerCase()+requestTopicSuffix; }

    public String constructOutputTopicName()
    { return (this.environmentPrefix+outputTopicSuffix).replace("..", "."); }

    public String constructClientOutputTopicName(String client)
    { return this.environmentPrefix+client.toLowerCase()+outputTopicSuffix; }

}
