package beerkafka;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.kafka.clients.producer.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Properties;

public class BeerKafkaProducer {


    public static void main(String[] args) {
        String bootstrapServers = "localhost:9092";
        String topic = "topic-beer";
        String FruitApiUrl1 = "https://api.openbrewerydb.org/v1/breweries?page=";
        String FruitApiUrl2 = "&per_page=1";
        String gotCharacters = "characters";

        // Set up Kafka producer properties
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (Producer<String, String> producer = new KafkaProducer<>(properties)) {
            for (int i = 1; i < 4000; i++) {
                try (CloseableHttpClient httpClient = HttpClients.createDefault();) {
                    HttpGet request = new HttpGet(FruitApiUrl1 + i + FruitApiUrl2);
                    HttpResponse response = httpClient.execute(request);

                    System.out.println(i);
                    System.out.println(response);

                    BufferedReader reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));

                    String line;

                    while ((line = reader.readLine()) != null) {


                        ProducerRecord<String, String> record = new ProducerRecord<>(topic, line);


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

                    //  }
                }
            }
        }

    }
}



