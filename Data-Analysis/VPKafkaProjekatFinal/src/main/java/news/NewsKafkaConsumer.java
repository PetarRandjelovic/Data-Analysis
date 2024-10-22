package news;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.opencsv.CSVWriter;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.sql.SparkSession;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

public class NewsKafkaConsumer {

    static AtomicBoolean atomicBoolean=new AtomicBoolean(false);

    public static void main(String[] args) {





        String topic = "topic-news";
        String bootstrapServers = "localhost:9092";
        String groupId = "Veliki Podaci NEWS";


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

                    createCsv(records);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    private static void createCsv(ConsumerRecords<String, String> records) {
        // Specify the CSV file path
        String csvFilePath = "C:\\Users\\petar\\Desktop\\VP Projekat\\vestigotovotest1111.csv";
        File csvFile = new File(csvFilePath);

        // Create CsvMapper and CsvSchema with dynamic column names
        CsvMapper csvMapper = new CsvMapper();

        try (CSVWriter writer = new CSVWriter(new FileWriter(csvFile,true))) {
            // Access the first record to determine the header


            // Iterate through Kafka records and write to CSV
            records.forEach(record -> {

                String[] data=record.value().split("\n ");
                for(String s:data){
                //    System.out.println(s);


                    String m[]=s.split("\\{\"source\":");


                    ObjectMapper jsonMapper = new ObjectMapper();
                    try {
                        JsonNode rootNode = jsonMapper.readTree(s);

                        rootNode=rootNode.path("articles");

                     //   rootNode.set("source", rootNode.path("source").path("name"));

                        rootNode.elements().forEachRemaining(jsonNode -> {
                            ( (ObjectNode) jsonNode).set("source", jsonNode.path("source").path("name"));
                         //   ( (ObjectNode) jsonNode).remove("description");
                         //   ( (ObjectNode) jsonNode).remove("content");


                        });


                        CsvSchema csvSchema=createSchema().withoutQuoteChar();

                      //  System.out.println(csvSchema.getColumnNames());

                        String[] csvData = csvMapper.writerFor(JsonNode.class).with(csvSchema).writeValueAsString(rootNode).split("[\",\n\"]");


                        System.out.println(csvData);

                        writer.writeNext(csvData);





                      //     writer.writeNext(au);

                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }



                    String au[]=new String[99];
                    for(int i=2;i<101;i++) {




                        if(i==100){
                            atomicBoolean.set(true);
                        }else{
                         //   System.out.println(i);
                        }
                        int startIndex = m[i].indexOf('{');
                        int endIndex = m[i].indexOf('}');

                        // Remove the first "{" and "}" from the string
                        String outputString = m[i].substring(startIndex + 1, endIndex) + m[i].substring(endIndex + 1);
                       m[i] = outputString;
                   //     System.out.println(m[i]);
                au[i-2]=m[i];

                        au[i-2]=au[i-2].replaceAll(",\".*?\":",",");
                        au[i-2]=au[i-2].replaceAll("\"id\":","");

                        au[i-2]=au[i-2].substring(0, au[i-2].length()-1);

                        au[i-2]=au[i-2]+"\n";

                     //   System.out.println(au[i-2]);




                        if(atomicBoolean.get()==true){
                           // System.out.println(m+" staaaa");



                        }



                    }
                }

            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static CsvSchema createSchema() {
        CsvSchema.Builder csvSchemaBuilder = CsvSchema.builder();
        csvSchemaBuilder.addColumn("source");
        csvSchemaBuilder.addColumn("author");
        csvSchemaBuilder.addColumn("title");
        csvSchemaBuilder.addColumn("url");
        csvSchemaBuilder.addColumn("urlToImage");
        csvSchemaBuilder.addColumn("publishedAt");
        csvSchemaBuilder.addColumn("content");
        csvSchemaBuilder.addColumn("description");

        return csvSchemaBuilder.build();

    }






}
