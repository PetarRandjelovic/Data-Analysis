package spark;

import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.opencsv.CSVWriter;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.FileWriter;
import java.util.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.opencsv.CSVWriter;
import org.apache.kafka.clients.consumer.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;


public class SparkKafkaConsumer {


    public static void main(String[] args) {



        String topic = "topic-wikimedia";
        String bootstrapServers = "localhost:9092";
        String groupId = "Veliki Podaci GOT";


        // Set up Kafka consumer properties
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());



        try (Consumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            // Subscribe to the Kafka topic
            consumer.subscribe(Collections.singletonList(topic));

            // Process Kafka records and write to CSV
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);

                if (!records.isEmpty()) {
                    // Write Kafka records to CSV

                    createCsv(records);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }



    }

    private static void createCsv(ConsumerRecords<String, String> records) {
        // Specify the CSV file path
        String csvFilePath = "C:\\Users\\petar\\Desktop\\VP Projekat\\output2.csv";
        File csvFile = new File(csvFilePath);

        // Create CsvMapper and CsvSchema with dynamic column names
        CsvMapper csvMapper = new CsvMapper();

        try (CSVWriter writer = new CSVWriter(new FileWriter(csvFile,true))) {
            // Access the first record to determine the header



            // Iterate through Kafka records and write to CSV
            records.forEach(record -> {

                String[] data=record.value().split("\n ");
                for(String s:data){
                    if(s.startsWith("data:")){
                        s=s.substring(5);

                     //   System.out.println(s+" TESTHEH");

                        try {

                            ObjectMapper jsonMapper = new ObjectMapper();
                            JsonNode rootNode = jsonMapper.readTree(s);
                            flattenNestedStructures((ObjectNode) rootNode);
                            CsvSchema csvSchema = createDynamicCsvSchema(rootNode).withoutQuoteChar();

                          //  System.out.println(rootNode+" HEJ TEST");

                            String[] csvData = convertJsonNodeToCsvRow(csvMapper, csvSchema, rootNode);
                            writer.writeNext(csvData);
                           // String[] csvData = csvMapper.writerFor(JsonNode.class).with(csvSchema).writeValueAsString(rootNode).replaceAll("[\"'\\]\\[\n]","").split("[,\n]");
                          //  System.out.println(Arrays.toString(csvData));
                          //  writer.writeNext(csvData);

                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }

            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String[] convertJsonNodeToCsvRow(CsvMapper csvMapper, CsvSchema csvSchema, JsonNode rootNode) throws IOException {
        String csvRow = csvMapper.writerFor(JsonNode.class).with(csvSchema).writeValueAsString(rootNode);
        return csvRow.replaceAll("[\"'\\]\\[\n]", "").split("[,\n]");
    }

    private static CsvSchema createDynamicCsvSchema(JsonNode hoursArray) {
        CsvSchema.Builder csvSchemaBuilder = CsvSchema.builder();
        Iterator<String> fieldNames = hoursArray.fieldNames();
        for (Iterator<String> it = fieldNames; it.hasNext(); ) {
            String fieldName = it.next();
            csvSchemaBuilder.addColumn(fieldName);

        }
        return csvSchemaBuilder.build();
    }

    private static void flattenNestedStructures(ObjectNode node) {
        node.set("length", node.path("length").path("old"));
        node.set("length_new", node.path("length").path("new"));
        node.set("revision", node.path("revision").path("old"));
        node.set("revision_new", node.path("revision").path("new"));

        node.get("meta").fieldNames().forEachRemaining(s -> {
            node.set(s, node.path("meta").path(s));
        });
        node.remove("meta");
        System.out.println(node.toPrettyString());

    }

}
