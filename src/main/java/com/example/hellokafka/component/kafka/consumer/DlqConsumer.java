package com.example.hellokafka.component.kafka.consumer;

import com.example.hellokafka.dto.DlqMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class DlqConsumer {

    private final ObjectMapper objectMapper;

    @KafkaListener(
        topics = "${kafka.topics.dlqTopic}",
        groupId = "dlq-consumer",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeDlq(String messageJson, Acknowledgment acknowledgment) {
        try {
            log.info("DLQ received message: {}", messageJson);
            DlqMessage message = objectMapper.readValue(messageJson, DlqMessage.class);
            log.info("DLQ processing message: ID={}, Error={}, From={}",
                message.getId(), message.getMessage(), message.getFrom());

            // Логика обработки DLQ сообщений

            acknowledgment.acknowledge();
        } catch (Exception e) {
            log.error("Error in DLQ consumer: {}", e.getMessage(), e);
            acknowledgment.acknowledge();
        }
    }
}