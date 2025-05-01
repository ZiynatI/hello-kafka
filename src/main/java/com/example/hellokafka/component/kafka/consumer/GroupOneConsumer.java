package com.example.hellokafka.component.kafka.consumer;

import com.example.hellokafka.dto.Message;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class GroupOneConsumer {

    private final ObjectMapper objectMapper;

    @KafkaListener(
        topics = "${kafka.topics.mainTopic}",
        groupId = "group-1",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(String messageJson, Acknowledgment acknowledgment) {
        try {
            log.info("Group-1 received message: {}", messageJson);
            Message message = objectMapper.readValue(messageJson, Message.class);
            log.info("Group-1 processing message with ID: {}", message.getId());

            // Бизнес-логика обработки

            acknowledgment.acknowledge();
        } catch (Exception e) {
            log.error("Error in Group-1 consumer: {}", e.getMessage(), e);
            acknowledgment.acknowledge(); // Подтверждаем даже при ошибке, в реальном сценарии может быть другая логика
        }
    }
}