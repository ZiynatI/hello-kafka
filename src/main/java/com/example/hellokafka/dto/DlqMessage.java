package com.example.hellokafka.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DlqMessage {
    private String id;
    private String from;
    private String message;
    private String value;
    private String serviceName;
    private LocalDateTime timestamp;
}