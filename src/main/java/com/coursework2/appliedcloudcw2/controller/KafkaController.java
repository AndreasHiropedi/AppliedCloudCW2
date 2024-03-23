package com.coursework2.appliedcloudcw2.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class KafkaController {

    @PostMapping("/readTopic/{topicName}")
    public String readTopic(@PathVariable String topicName) {
        // Implement the logic to read from the specified Kafka topic
        // Return the data as a String
        return "Data from topic " + topicName;
    }

    @PostMapping("/writeTopic/{topicName}/{data}")
    public ResponseEntity<?> writeTopic(@PathVariable String topicName, @PathVariable String data) {
        // Implement the logic to write data to the specified Kafka topic
        return ResponseEntity.ok().build();
    }

    @PostMapping("/transformMessage/{readTopic}/{writeTopic}")
    public ResponseEntity<?> transformMessage(@PathVariable String readTopic, @PathVariable String writeTopic) {
        // 1. Read data from readTopic
        // 2. Transform the data (e.g., to uppercase)
        // 3. Write transformed data to writeTopic
        return ResponseEntity.ok().build();
    }

    @PostMapping("/store/{readTopic}/{writeTopic}")
    public ResponseEntity<?> store(@PathVariable String readTopic, @PathVariable String writeTopic, @RequestBody Map<String, String> properties) {
        // 1. Read data from readTopic
        // 2. Write it to a BLOB using the storage service
        // 3. Write the storage service's response UUID to writeTopic
        return ResponseEntity.ok().build();
    }

    @PostMapping("/retrieve/{writeTopic}/{uuid}")
    public ResponseEntity<?> retrieve(@PathVariable String writeTopic, @PathVariable String uuid) {
        // 1. Read the BLOB for the given UUID
        // 2. Write the JSON structure into the writeTopic
        return ResponseEntity.ok().build();
    }
}

