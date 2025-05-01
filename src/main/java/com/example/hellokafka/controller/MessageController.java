package com.example.hellokafka.controller;

import com.example.hellokafka.component.kafka.producer.KafkaProducer;
import com.example.hellokafka.dto.Message;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.util.UUID;

@RestController
@RequestMapping("/api/messages")
@RequiredArgsConstructor
public class MessageController {

    private final KafkaProducer kafkaProducer;

    @PostMapping
    public Message sendMessage(@RequestBody Message messageRequest) {
        Message message = new Message(
            UUID.randomUUID().toString(),
            messageRequest.getContent(),
            LocalDateTime.now()
        );

        kafkaProducer.sendMessage(message);
        return message;
    }
}
