package com.coursework2.appliedcloudcw2.controller;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@RestController
public class KafkaController {
    private final String storageServerBaseURL = "http://acp-storage.azurewebsites.net/";
    private final String key = "S2015345";

    private final String configFileName = "src/main/resources/application.properties";

    public static Properties loadConfig(final String configFile) throws IOException
    {
        if (!Files.exists(Paths.get(configFile))) {
            throw new IOException(configFile + " not found.");
        }
        final Properties cfg = new Properties();
        try (InputStream inputStream = new FileInputStream(configFile)) {
            cfg.load(inputStream);
        }
        return cfg;
    }

    //============================= SERVICE ENDPOINTS =============================//

    @PostMapping("/readTopic/{topicName}")
    public String readFromTopic(@PathVariable String topicName) throws IOException
    {
        // TODO: FIX THIS

        // Create consumer
        Properties kafkaPros = loadConfig(configFileName);
        kafkaPros.put("group.id", "acp-cw2");
        var consumer = new KafkaConsumer<String, String>(kafkaPros);

        // Retrieve topic and messages for that topic
        consumer.subscribe(Collections.singletonList(topicName));
        StringBuilder resultBuilder = new StringBuilder();
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

        System.out.println(records.count());

        // Output messages for that topic
        for (ConsumerRecord<String, String> record : records) {
            System.out.println(String.format("[%s] %s: %s %s %s %s", record.topic(), record.key(), record.value(), record.partition(), record.offset(), record.timestamp()));
            resultBuilder.append(record.value()).append("\n");  // Appending each message to the result
        }

        return resultBuilder.toString();
    }

    @PostMapping("/writeTopic/{topicName}/{data}")
    public ResponseEntity<Void> writeToTopic (
            @PathVariable String topicName,
            @PathVariable String data
    ) throws IOException
    {

        // Create Producer
        Properties kafkaPros = loadConfig(configFileName);
        kafkaPros.put("group.id", "acp-cw2");
        var producer = new KafkaProducer<String, String>(kafkaPros);

        // Write message to topic
        producer.send(new ProducerRecord<>(topicName, key, data), (recordMetadata, ex) -> {
            if (ex != null)
                ex.printStackTrace();
            else
                System.out.printf("Produced event to topic %s: key = %-10s value = %s%n", topicName, key, data);
        });

        return ResponseEntity.ok().build();
    }

    @PostMapping("/transformMessage/{readTopic}/{writeTopic}")
    public ResponseEntity<Void> transformMessage(
            @PathVariable String readTopic,
            @PathVariable String writeTopic
    ) throws IOException
    {

        // Read data from readTopic
        String data = readFromTopic(readTopic);

        // Transform to uppercase
        String transformedData = data.toUpperCase();

        // Write transformed data to writeTopic
        writeToTopic(writeTopic, transformedData);

        return ResponseEntity.ok().build();
    }

    @PostMapping("/store/{readTopic}/{writeTopic}")
    public ResponseEntity<Void> store(
            @PathVariable String readTopic,
            @PathVariable String writeTopic,
            @RequestHeader Properties properties
    ) throws IOException
    {

        // TODO: IMPLEMENT THIS

        /*
        String storageServerBaseUrl = properties.getProperty("storage.server");
        // Create an url by appending "/write/blob" to the storage server base url
        String writeBlobUrl = storageServerBaseUrl + "/write/blob";

        // Read data from readTopic
        String data = readMessage(readTopic);

        // Create JSON payload
        String jsonData = String.format("{ \"uid\": \"%s\", \"datasetName\": \"%s\", \"data\": \"%s\" }", key, "acp_coursework2", data);

        // Set headers
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        // Set request entity
        HttpEntity<String> requestEntity = new HttpEntity<>(jsonData, headers);

        // Create RestTemplate
        RestTemplate restTemplate = new RestTemplate();

        // Send POST request
        ResponseEntity<Void> response = restTemplate.exchange(writeBlobUrl, HttpMethod.POST, requestEntity, Void.class);

        // Check the response status
        if (response.getStatusCode() == HttpStatus.OK)
        {
            String uuid = response.getBody().toString();
            // Write uuid to writeTopic
            sendMessage(writeTopic, key, uuid);
        }
        else
        {
            return ResponseEntity.status(response.getStatusCode()).build();
        }

         */
        return ResponseEntity.ok().build();
    }

    @PostMapping("/retrieve/{writeTopic}/{uuid}")
    public ResponseEntity<Void> retrieve (@PathVariable String writeTopic, @PathVariable String uuid) {

        // TODO: IMPLEMENT THIS

        return ResponseEntity.ok().build();
    }

}

