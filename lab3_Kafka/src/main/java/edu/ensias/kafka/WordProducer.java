package edu.ensias.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;
import java.util.Scanner;

public class WordProducer {
    public static void main(String[] args) {
        if(args.length == 0){
            System.out.println("Entrer le nom du topic");
            return;
        }
        
        String topicName = args[0].toString();
        
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        
        Producer<String, String> producer = new KafkaProducer<>(props);
        Scanner scanner = new Scanner(System.in);
        
        System.out.println("=== Word Producer - Kafka ===");
        System.out.println("Topic: " + topicName);
        System.out.println("Entrez des mots (Ctrl+C pour quitter):");
        
        try {
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                if (line != null && !line.trim().isEmpty()) {
                    producer.send(new ProducerRecord<>(topicName, null, line));
                    System.out.println("Envoyé: " + line);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            scanner.close();
            producer.close();
            System.out.println("Producer fermé.");
        }
    }
}
