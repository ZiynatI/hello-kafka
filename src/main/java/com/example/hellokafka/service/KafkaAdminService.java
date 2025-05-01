package com.example.hellokafka.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaAdminService {

    // Включение/выключение брокера через Docker
    public boolean toggleBroker(int brokerId, boolean start) {
        String action = start ? "start" : "stop";
        String containerName = "kafka" + brokerId;

        try {
            Process process = Runtime.getRuntime().exec(
                new String[]{"docker", action, containerName}
            );

            int exitCode = process.waitFor();
            log.info("Docker {} command for container {} executed with exit code: {}",
                action, containerName, exitCode);

            return exitCode == 0;
        } catch (IOException | InterruptedException e) {
            log.error("Error executing docker command: {}", e.getMessage(), e);
            return false;
        }
    }

    // Остановка брокера
    public boolean stopBroker(int brokerId) {
        return toggleBroker(brokerId, false);
    }

    // Запуск брокера
    public boolean startBroker(int brokerId) {
        return toggleBroker(brokerId, true);
    }
}
