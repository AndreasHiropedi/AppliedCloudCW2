package com.coursework2.appliedcloudcw2.controller;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@RestController
public class KafkaController {
    private final String key = "S2015345";

    @PostMapping("/readTopic/{topicName}")
    public ResponseEntity<String> readFromTopic(
            @PathVariable String topicName,
            @RequestBody List<Map<String, String>> requestBody
    )
    {

        try {
            // Get the properties from the request body
            Properties kafkaPros = new Properties();
            for (Map<String, String> map : requestBody) {
                kafkaPros.putAll(map);
            }

            // Set serializer and deserializer configurations
            kafkaPros.put("key.serializer", StringSerializer.class.getName());
            kafkaPros.put("value.serializer", StringSerializer.class.getName());
            kafkaPros.put("key.deserializer", StringDeserializer.class.getName());
            kafkaPros.put("value.deserializer", StringDeserializer.class.getName());

            // Create consumer
            try (Consumer<String, String> consumer = new KafkaConsumer<>(kafkaPros)) {
                // Retrieve topic and messages for that topic
                consumer.subscribe(Collections.singletonList(topicName));
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                String lastRecord = null;

                // Get the last message for that topic
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(String.format("[%s] %s: %s %s %s %s", record.topic(), record.key(), record.value(), record.partition(), record.offset(), record.timestamp()));
                    lastRecord = record.value();
                }

                if (lastRecord != null) {
                    return ResponseEntity.ok(lastRecord);
                }

                return ResponseEntity.badRequest().build();

            } catch (Exception exception) {

                return ResponseEntity.badRequest().build();

            }

        } catch (Exception exception) {

            return ResponseEntity.badRequest().build();

        }

    }

    @PostMapping("/writeTopic/{topicName}/{data}")
    public ResponseEntity<Void> writeToTopic (
            @PathVariable String topicName,
            @PathVariable String data,
            @RequestBody List<Map<String, String>> requestBody
    )
    {

        try {
            // Get the properties from the request body
            Properties kafkaPros = new Properties();
            for (Map<String, String> map : requestBody) {
                kafkaPros.putAll(map);
            }

            // Set serializer and deserializer configurations
            kafkaPros.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            kafkaPros.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            kafkaPros.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            kafkaPros.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

            // Create Producer
            var producer = new KafkaProducer<String, String>(kafkaPros);

            // Write message to topic
            producer.send(new ProducerRecord<>(topicName, key, data), (recordMetadata, ex) -> {
                if (ex != null) {
                    ex.printStackTrace();
                }
                else
                    System.out.printf("Produced event to topic %s: key = %-10s value = %s%n", topicName, key, data);
            });

            return ResponseEntity.ok().build();

        } catch (Exception exception) {

            return ResponseEntity.badRequest().build();

        }
    }

    @PostMapping("/transformMessage/{readTopic}/{writeTopic}")
    public ResponseEntity<Void> transformMessage(
            @PathVariable String readTopic,
            @PathVariable String writeTopic,
            @RequestBody List<Map<String, String>> requestBody
    )
    {
        try {

            // Get the properties from the request body
            Properties kafkaPros = new Properties();
            for (Map<String, String> map : requestBody) {
                kafkaPros.putAll(map);
            }

            // Set serializer and deserializer configurations
            kafkaPros.put("key.serializer", StringSerializer.class.getName());
            kafkaPros.put("value.serializer", StringSerializer.class.getName());
            kafkaPros.put("key.deserializer", StringDeserializer.class.getName());
            kafkaPros.put("value.deserializer", StringDeserializer.class.getName());

            // Create consumer
            try (Consumer<String, String> consumer = new KafkaConsumer<>(kafkaPros)) {
                // Retrieve topic and messages for that topic
                consumer.subscribe(Collections.singletonList(readTopic));
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
                String lastRecord = null;

                // Get the last message for that topic
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(String.format("[%s] %s: %s %s %s %s", record.topic(), record.key(), record.value(), record.partition(), record.offset(), record.timestamp()));
                    lastRecord = record.value();
                }

                if (lastRecord != null) {

                    // Create Producer
                    var producer = new KafkaProducer<String, String>(kafkaPros);

                    // Send upper case message
                    String upperCaseMessage = lastRecord.toUpperCase();
                    producer.send(new ProducerRecord<>(writeTopic, key, upperCaseMessage));

                    return ResponseEntity.ok().build();
                }

                return ResponseEntity.badRequest().build();

            } catch (Exception exception) {

                return ResponseEntity.badRequest().build();

            }

        } catch (Exception exception) {

            return ResponseEntity.badRequest().build();

        }

    }

    @PostMapping("/store/{readTopic}/{writeTopic}")
    public ResponseEntity<Void> store(
            @PathVariable String readTopic,
            @PathVariable String writeTopic,
            @RequestBody List<Map<String, String>> requestBody
    )
    {

        // Get the properties from the request body
        Properties kafkaPros = new Properties();
        for (Map<String, String> map : requestBody) {
            kafkaPros.putAll(map);
        }

        // Set serializer and deserializer configurations
        kafkaPros.put("key.serializer", StringSerializer.class.getName());
        kafkaPros.put("value.serializer", StringSerializer.class.getName());
        kafkaPros.put("key.deserializer", StringDeserializer.class.getName());
        kafkaPros.put("value.deserializer", StringDeserializer.class.getName());

        try {

            String storageServerBaseUrl = kafkaPros.getProperty("storage.server");

            // Create an url by appending "/write/blob" to the storage server base url
            String writeBlobUrl = storageServerBaseUrl;
            if (storageServerBaseUrl.charAt(storageServerBaseUrl.length()-1) == '/')
            {
                writeBlobUrl += "write/blob";
            }
            else
            {
                writeBlobUrl += "/write/blob";
            }

            // Create consumer
            try (Consumer<String, String> consumer = new KafkaConsumer<>(kafkaPros)) {
                // Data from readTopic
                String data;

                // Retrieve topic and messages for that topic
                consumer.subscribe(Collections.singletonList(readTopic));
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                String lastRecord = null;

                // Get the last message for that topic
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(String.format("[%s] %s: %s %s %s %s", record.topic(), record.key(), record.value(), record.partition(), record.offset(), record.timestamp()));
                    lastRecord = record.value();
                }

                if (lastRecord != null) {

                    data = lastRecord;

                    // Create JSON payload
                    String jsonData = String.format("{ \"uid\": \"%s\", \"datasetName\": \"%s\", \"data\": \"%s\" }", key, "acp-cw2", data);

                    // Set the headers
                    HttpHeaders headers = new HttpHeaders();
                    headers.setContentType(MediaType.APPLICATION_JSON);

                    // Set the request entity
                    HttpEntity<String> requestEntity = new HttpEntity<>(jsonData, headers);

                    // Create the Rest template
                    RestTemplate restTemplate = new RestTemplate();

                    // Send the POST request
                    ResponseEntity<Void> response = restTemplate.exchange(writeBlobUrl, HttpMethod.POST, requestEntity, Void.class);

                    // Check the response status
                    if (response.getStatusCode() == HttpStatus.OK)
                    {

                        String uuid = response.getBody().toString();

                        // Create producer to write uuid to writeTopic
                        try (var producer = new KafkaProducer<String, String>(kafkaPros);) {

                            // Write message
                            producer.send(new ProducerRecord<>(writeTopic, key, uuid), (recordMetadata, ex) -> {
                                if (ex != null) {
                                    ex.printStackTrace();
                                }
                                else
                                    System.out.printf("Produced event to topic %s: key = %-10s value = %s%n", writeTopic, key, uuid);
                            });

                            return ResponseEntity.ok().build();

                        } catch (Exception exception) {

                            return ResponseEntity.badRequest().build();

                        }

                    }
                    else
                    {
                        return ResponseEntity.badRequest().build();
                    }

                }

                // If no topic found
                else return ResponseEntity.badRequest().build();

            } catch (Exception exception) {

                return ResponseEntity.badRequest().build();

            }

        } catch (Exception exception) {

            return ResponseEntity.badRequest().build();

        }
    }

    @PostMapping("/retrieve/{writeTopic}/{uuid}")
    public ResponseEntity<Void> retrieve(
            @PathVariable String writeTopic,
            @PathVariable String uuid,
            @RequestBody List<Map<String, String>> requestBody)
    {

        // Get the properties from the request body
        Properties kafkaPros = new Properties();
        for (Map<String, String> map : requestBody) {
            kafkaPros.putAll(map);
        }

        // Set serializer and deserializer configurations
        kafkaPros.put("key.serializer", StringSerializer.class.getName());
        kafkaPros.put("value.serializer", StringSerializer.class.getName());
        kafkaPros.put("key.deserializer", StringDeserializer.class.getName());
        kafkaPros.put("value.deserializer", StringDeserializer.class.getName());

        try {

            String storageServerBaseUrl = kafkaPros.getProperty("storage.server");

            // Create an url by appending "/write/blob" to the storage server base url
            String readBlobUrl = storageServerBaseUrl;
            if (storageServerBaseUrl.charAt(storageServerBaseUrl.length()-1) == '/')
            {
                readBlobUrl += "read/blob/" + uuid;
            }
            else
            {
                readBlobUrl += "/read/blob/" + uuid;
            }

            // Send request to Blob
            RestTemplate restTemplate = new RestTemplate();
            ResponseEntity<byte[]> blobResponse = restTemplate.getForEntity(readBlobUrl, byte[].class);

            // Check if response from Rest service was 200 (OK)
            if (!blobResponse.getStatusCode().is2xxSuccessful()) {
                return ResponseEntity.badRequest().build();
            }

            String blobData = blobResponse.getBody().toString();

            // Create producer
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaPros)) {

                // Write BLOB data to writeTopic
                ProducerRecord<String, String> record = new ProducerRecord<>(writeTopic, key, blobData);
                producer.send(record);
                return ResponseEntity.ok().build();

            } catch (Exception e) {

                return ResponseEntity.badRequest().build();

            }

        } catch (Exception exception) {

            return ResponseEntity.badRequest().build();

        }
    }

}
