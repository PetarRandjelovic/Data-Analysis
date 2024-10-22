package beerkafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.opencsv.CSVWriter;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.*;
import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class BeerKafkaConsumer {
    static AtomicBoolean atomicBoolean=new AtomicBoolean(true);

    public static void main(String[] args) {



        String topic = "topic-beer";
        String bootstrapServers = "localhost:9092";
        String groupId = "Veliki Podaci Beer";


        // Set up Kafka consumer properties
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
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
              //  System.out.println(records);

                if (!records.isEmpty()) {
                    // Write Kafka records to CSV
                  //  System.out.println("s1");
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
        // Specify the CSV file path
        String csvFilePath = "C:\\Users\\petar\\Desktop\\VP Projekat\\beer1.csv";
     //   String csvFilePath = "output.csv";
    //    System.out.println("??");


        try (CSVWriter csvWriter = new CSVWriter(new FileWriter(csvFilePath,true))) {
            // Write CSV header
            if(atomicBoolean.get()==true) {
                String[] header = {"id", "name", "brewery_type", "address_1", "address_2", "address_3",
                        "city", "state_province", "postal_code", "country", "longitude", "latitude",
                        "phone", "website_url", "state", "street"};
                csvWriter.writeNext(header);
                atomicBoolean.set(false);
            }
            // Iterate over records and write CSV rows
            for (ConsumerRecord<String, String> record : records) {
                String jsonString = record.value();

                // Parse JSON string to extract values
                ObjectMapper objectMapper = new ObjectMapper();
                JsonNode jsonNodes = objectMapper.readTree(jsonString);

              //  System.out.println(jsonNode);

                for (JsonNode jsonNode : jsonNodes) {



                    csvWriter.writeNext(new String[] {
                            jsonNode.get("id").asText(),
                            jsonNode.get("name").asText(),
                            jsonNode.get("brewery_type").asText(),
                            jsonNode.get("address_1").asText(),
                            jsonNode.get("address_2").asText(),
                            jsonNode.get("address_3").asText(),
                            jsonNode.get("city").asText(),
                            jsonNode.get("state_province").asText(),
                            jsonNode.get("postal_code").asText(),
                            jsonNode.get("country").asText(),
                            jsonNode.get("longitude").asText(),
                            jsonNode.get("latitude").asText(),
                            jsonNode.get("phone").asText(),
                            jsonNode.get("website_url").asText(),
                            jsonNode.get("state").asText(),
                            jsonNode.get("street").asText()
                    });
                }
              //  csvWriter.writeNext(row);
            }

         //   System.out.println("CSV file created successfully.");

        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("CSV file created successfully.");
    }




}
