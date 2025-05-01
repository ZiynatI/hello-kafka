package com.example.hellokafka.configuration.props;

import lombok.AccessLevel;
import lombok.Data;
import lombok.experimental.FieldDefaults;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "kafka")
@FieldDefaults(level = AccessLevel.PRIVATE)
public class KafkaProps {

    String groupId;
    String clientId;
    String acksConfig;
    String clientDnsLookup;
    String bootstrapServers;
    String autoOffsetResetConfig;

    int retriesConfig;
    int lingerMsConfig;
    int batchSizeConfig;
    int bufferMemoryConfig;
    String compressionType;

    Topic topics;

    @Data
    @FieldDefaults(level = AccessLevel.PRIVATE)
    public static class Topic {
        String mainTopic;
        String dlqTopic;
    }
}