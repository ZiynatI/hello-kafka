package com.example.hellokafka.controller;

import com.example.hellokafka.component.kafka.producer.KafkaProducer;
import com.example.hellokafka.dto.Message;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("/api/kafka")
@RequiredArgsConstructor
public class KafkaController {

    private final KafkaProducer kafkaProducer;
    private final KafkaAdmin kafkaAdmin;

    @Qualifier("mainTopicProducer")
    private final KafkaTemplate<String, Message> mainTopicTemplate;

    // Отправка обычного сообщения
    @PostMapping("/send")
    public ResponseEntity<Message> sendMessage(@RequestBody Message message) {
        if (message.getId() == null || message.getId().isEmpty()) {
            message.setId(UUID.randomUUID().toString());
        }

        if (message.getTimestamp() == null) {
            message.setTimestamp(LocalDateTime.now());
        }

        kafkaProducer.sendMessage(message);
        return ResponseEntity.ok(message);
    }

    // Отправка сообщения, которое вызовет ошибку и попадет в DLQ
    @PostMapping("/send-error")
    public ResponseEntity<Message> sendErrorMessage(@RequestParam(defaultValue = "Test error") String errorMessage) {
        Message message = new Message(
            UUID.randomUUID().toString(),
            "ERROR: " + errorMessage,  // префикс ERROR вызовет ошибку в консьюмере
            LocalDateTime.now()
        );

        kafkaProducer.sendMessage(message);
        return ResponseEntity.ok(message);
    }

    // Отправка множества сообщений для тестирования батчинга
    @PostMapping("/send-batch")
    public ResponseEntity<Map<String, Object>> sendBatch(@RequestParam(defaultValue = "10") int count) {
        for (int i = 0; i < count; i++) {
            Message message = new Message(
                UUID.randomUUID().toString(),
                "Batch message #" + i,
                LocalDateTime.now()
            );

            kafkaProducer.sendMessage(message);
        }

        return ResponseEntity.ok(Map.of(
            "status", "success",
            "count", count,
            "message", "Batch messages sent"
        ));
    }

    // Получение списка топиков для проверки их создания
    @GetMapping("/topics")
    public ResponseEntity<Set<String>> listTopics() throws ExecutionException, InterruptedException {
        try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            ListTopicsResult topics = adminClient.listTopics();
            return ResponseEntity.ok(topics.names().get());
        }
    }

    // Изменение настроек продюсера
    @PostMapping("/config")
    public ResponseEntity<Map<String, Object>> updateConfig(
        @RequestParam(required = false) String compressionType,
        @RequestParam(required = false) Integer batchSize,
        @RequestParam(required = false) Integer lingerMs) {

        Map<String, Object> configs = new HashMap<>();
        Map<String, Object> result = new HashMap<>();

        if (compressionType != null) {
            configs.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);
            result.put("compressionType", compressionType);
        }

        if (batchSize != null) {
            configs.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
            result.put("batchSize", batchSize);
        }

        if (lingerMs != null) {
            configs.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs);
            result.put("lingerMs", lingerMs);
        }

        if (!configs.isEmpty()) {
            // Обновляем конфигурацию продюсера
            updateProducerConfig(configs);
            result.put("status", "updated");
        } else {
            result.put("status", "no changes");
        }

        return ResponseEntity.ok(result);
    }

    // Вспомогательный метод для обновления конфигурации продюсера
    private void updateProducerConfig(Map<String, Object> configs) {
        if (mainTopicTemplate.getProducerFactory() instanceof org.springframework.kafka.core.DefaultKafkaProducerFactory) {
            ((org.springframework.kafka.core.DefaultKafkaProducerFactory<?, ?>) mainTopicTemplate.getProducerFactory())
                .updateConfigs(configs);
        }
    }
}