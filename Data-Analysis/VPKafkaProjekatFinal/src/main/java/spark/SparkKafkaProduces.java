package spark;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.kafka.clients.producer.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Properties;

public class SparkKafkaProduces {


    public static void main(String[] args) {
        String bootstrapServers = "localhost:9092";
        String topic = "topic-wikimedia";
        String wikimediaApiUrl = "https://stream.wikimedia.org/v2/stream/recentchange";
        String gotCharacters = "characters";

        // Set up Kafka producer properties
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (Producer<String, String> producer = new KafkaProducer<>(properties)) {

                try (CloseableHttpClient httpClient = HttpClients.createDefault();) {
                    HttpGet request = new HttpGet(wikimediaApiUrl);
                    HttpResponse response = httpClient.execute(request);


                    BufferedReader reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));

                    // Process the response here
                    //   System.out.println(response + " " + i);

                    // Produce a Kafka message
                    String line;

                    while ((line = reader.readLine()) != null ) {

                        ProducerRecord<String, String> record = new ProducerRecord<>(topic, line);
                           //      System.out.println(line);
                        // Send the message to Kafka
                        producer.send(record, new Callback() {
                            @Override
                            public void onCompletion(RecordMetadata metadata, Exception exception) {
                                if (exception == null) {
                                    System.out.println("Message sent successfully to  " + metadata.topic());
                                    System.out.println("Partition: " + metadata.partition());
                                    System.out.println("Offset: " + metadata.offset());
                                } else {
                                    System.err.println("Error sending message: " + exception.getMessage());
                                }
                            }
                        });

                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
