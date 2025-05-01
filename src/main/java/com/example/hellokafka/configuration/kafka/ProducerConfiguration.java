package com.example.hellokafka.configuration.kafka;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;
import com.example.hellokafka.configuration.props.KafkaProps;
import com.example.hellokafka.dto.Message;
import com.example.hellokafka.dto.DlqMessage;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Configuration
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class ProducerConfiguration {

    KafkaProps kafkaProps;

    @Bean
    public Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, (kafkaProps.getClientId() + UUID.randomUUID()));
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProps.getBootstrapServers());
        props.put(ProducerConfig.CLIENT_DNS_LOOKUP_CONFIG, kafkaProps.getClientDnsLookup());

        props.put(ProducerConfig.ACKS_CONFIG, kafkaProps.getAcksConfig());
        props.put(ProducerConfig.RETRIES_CONFIG, kafkaProps.getRetriesConfig());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        props.put(ProducerConfig.BATCH_SIZE_CONFIG, kafkaProps.getBatchSizeConfig());
        props.put(ProducerConfig.LINGER_MS_CONFIG, kafkaProps.getLingerMsConfig());
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, kafkaProps.getBufferMemoryConfig());
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, kafkaProps.getCompressionType());

        props.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        return props;
    }

    @Bean("mainTopicProducer")
    public KafkaTemplate<String, Message> mainTopicTemplate() {
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerConfigs()));
    }

    @Bean("dlqTopicProducer")
    public KafkaTemplate<String, DlqMessage> dlqTopicTemplate() {
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerConfigs()));
    }

    // Универсальный темплейт для строк (для обратной совместимости)
    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        Map<String, Object> configs = producerConfigs();
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(configs));
    }
}
