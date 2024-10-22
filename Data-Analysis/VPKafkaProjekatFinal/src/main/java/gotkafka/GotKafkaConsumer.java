package gotkafka;

import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVWriter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import java.io.File;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import com.opencsv.CSVWriter;

public class GotKafkaConsumer {


    public static void main(String[] args) {

        System.out.println("??????");

        String topic = "topic-GOT";
        String bootstrapServers = "localhost:9092";
        String groupId = "Veliki Podaci GOT";


        // Set up Kafka consumer properties
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Create Kafka consumer


        // Subscribe to the Kafka topic


        try (Consumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            // Subscribe to the Kafka topic
            consumer.subscribe(Collections.singletonList(topic));

            // Process Kafka records and write to CSV
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);

                if (!records.isEmpty()) {
                    // Write Kafka records to CSV
                   // System.out.println(records);
                    createCsv(records);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }


      //  consumer.subscribe(Collections.singletonList(topic));

        // Poll for messages


    }

    private static void createCsv(ConsumerRecords<String, String> records) {
     //   System.out.println("Omegalol1");
        String csvFilePath = "C:\\Users\\petar\\Desktop\\VP Projekat\\aaaaaaaaa.csv";
        File csvFile = new File(csvFilePath);

        // Create CsvMapper and CsvSchema with dynamic column names
        CsvMapper csvMapper = new CsvMapper();

        try (CSVWriter csvWriter = new CSVWriter(new FileWriter(csvFile, true))) {
            ObjectMapper objectMapper = new ObjectMapper();
          //  System.out.println("Omegalol");
            // Iterate through consumer records
            for (ConsumerRecord<String, String> record : records) {


                String jsonString = record.value();

                System.out.println(jsonString);
                // Parse JSON using Jackson
                JsonNode rootNode = objectMapper.readTree(jsonString);

                // Write data to CSV
                csvWriter.writeNext(new String[]{
                      rootNode.path("url").asText(),
                          rootNode.path("name").asText(),
                        rootNode.path("gender").asText(),
                         rootNode.path("culture").asText(),
                        rootNode.path("born").asText(),
                        rootNode.path("died").asText(),
                         String.join(",", rootNode.path("titles").findValuesAsText("")),
                          String.join(",", rootNode.path("aliases").findValuesAsText("")),
                         rootNode.path("father").asText(),
                           rootNode.path("mother").asText(),
                           rootNode.path("spouse").asText(),
                          String.join(",", rootNode.path("allegiances").findValuesAsText("")),
                           String.join(",", rootNode.path("books").findValuesAsText("")),
                             String.join(",", rootNode.path("povBooks").findValuesAsText("")),
                           String.join(",", rootNode.path("tvSeries").findValuesAsText("")),
                         String.join(",", rootNode.path("playedBy").findValuesAsText(""))
                });
            }

            System.out.println("CSV file updated successfully.");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


}
