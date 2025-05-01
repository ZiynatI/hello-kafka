package com.example.hellokafka.handler;

import com.example.hellokafka.component.kafka.producer.KafkaProducer;
import com.example.hellokafka.dto.DlqMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaErrorHandler implements CommonErrorHandler {

    private final KafkaProducer kafkaProducer;

    @Override
    public boolean handleOne(final Exception exception, final ConsumerRecord<?, ?> record,
                             final Consumer<?, ?> consumer, final MessageListenerContainer container) {
        log.error("Exception thrown with record", exception);
        var topic = Arrays.toString(container.getContainerProperties().getTopics());

        var dlqMessage = new DlqMessage(
            UUID.randomUUID().toString(),
            topic,
            exception.getMessage(),
            record.value() != null ? record.value().toString() : null,
            "HELLO_KAFKA_SERVICE",
            LocalDateTime.now()
        );

        kafkaProducer.sendToDlq(dlqMessage);
        log.info("Sent to DLQ: key={}, value={}", record.key(), record.value());

        return true;
    }

    @Override
    public void handleOtherException(final Exception exception, final Consumer<?, ?> consumer,
                                     final MessageListenerContainer container, final boolean batchListener) {
        log.error("Exception thrown", exception);

        if (exception instanceof RecordDeserializationException ex) {
            consumer.seek(ex.topicPartition(), ex.offset() + 1L);
            consumer.commitSync();
        }

        var topic = Arrays.toString(container.getContainerProperties().getTopics());
        var dlqMessage = new DlqMessage(
            UUID.randomUUID().toString(),
            topic,
            exception.getMessage(),
            null,
            "HELLO_KAFKA_SERVICE",
            LocalDateTime.now()
        );

        kafkaProducer.sendToDlq(dlqMessage);
    }
}