package com.example.hellokafka.component.kafka.producer;

import com.example.hellokafka.configuration.props.KafkaProps;
import com.example.hellokafka.dto.DlqMessage;
import com.example.hellokafka.dto.Message;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaProducer {

    @Qualifier("mainTopicProducer")
    private final KafkaTemplate<String, Message> mainTopicTemplate;

    @Qualifier("dlqTopicProducer")
    private final KafkaTemplate<String, DlqMessage> dlqTopicTemplate;

    private final KafkaProps kafkaProps;

    public void sendMessage(Message message) {
        mainTopicTemplate.send(kafkaProps.getTopics().getMainTopic(), message.getId(), message)
            .thenAccept(result -> log.info("Message sent successfully: {} to partition {}",
                message.getId(), result.getRecordMetadata().partition()))
            .exceptionally(ex -> {
                log.error("Failed to send message: {}", ex.getMessage());
                return null;
            });

        log.info("Message sent to topic: {}", kafkaProps.getTopics().getMainTopic());
    }

    public void sendToDlq(DlqMessage dlqMessage) {
        dlqTopicTemplate.send(kafkaProps.getTopics().getDlqTopic(), dlqMessage.getId(), dlqMessage)
            .thenAccept(result -> log.info("DLQ message sent successfully: {} to partition {}",
                dlqMessage.getId(), result.getRecordMetadata().partition()))
            .exceptionally(ex -> {
                log.error("Failed to send DLQ message: {}", ex.getMessage());
                return null;
            });

        log.info("Message sent to DLQ topic: {}", kafkaProps.getTopics().getDlqTopic());
    }
}