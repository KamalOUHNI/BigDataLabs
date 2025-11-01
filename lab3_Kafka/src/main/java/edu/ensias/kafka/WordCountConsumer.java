package edu.ensias.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class WordCountConsumer {
    public static void main(String[] args) {
        if(args.length == 0){
            System.out.println("Entrer le nom du topic");
            return;
        }
        
        String topicName = args[0].toString();
        
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "word-count-group");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topicName));
        
        Map<String, Integer> wordCounts = new HashMap<>();
        
        System.out.println("=== Word Count Consumer - Kafka ===");
        System.out.println("Topic: " + topicName);
        System.out.println("En attente des messages...\n");
        
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    String text = record.value();
                    String[] words = text.toLowerCase().split("\\W+");
                    
                    for (String word : words) {
                        if (!word.isEmpty()) {
                            wordCounts.put(word, wordCounts.getOrDefault(word, 0) + 1);
                        }
                    }
                    
                    System.out.println("\n=== Fréquence des mots ===");
                    wordCounts.forEach((word, count) -> 
                        System.out.println(word + ": " + count)
                    );
                    System.out.println("==========================\n");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}
