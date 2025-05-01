package com.example.hellokafka.controller;

import com.example.hellokafka.service.KafkaAdminService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/kafka/admin")
@RequiredArgsConstructor
public class KafkaAdminController {

    private final KafkaAdminService adminService;

    @PostMapping("/broker/{id}/stop")
    public ResponseEntity<Map<String, Object>> stopBroker(@PathVariable int id) {
        boolean success = adminService.stopBroker(id);
        return ResponseEntity.ok(Map.of(
            "action", "stop",
            "brokerId", id,
            "success", success
        ));
    }

    @PostMapping("/broker/{id}/start")
    public ResponseEntity<Map<String, Object>> startBroker(@PathVariable int id) {
        boolean success = adminService.startBroker(id);
        return ResponseEntity.ok(Map.of(
            "action", "start",
            "brokerId", id,
            "success", success
        ));
    }
}