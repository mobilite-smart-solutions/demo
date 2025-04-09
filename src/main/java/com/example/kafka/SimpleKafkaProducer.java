package com.example.kafka;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;


public class SimpleKafkaProducer {
	static int i;

	public static void main(String[] args) {
		Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");  // Kafka broker address
		//props.put("bootstrap.servers", "65.2.175.140:9094");  // Kafka broker address
        props.put("key.serializer", StringSerializer.class.getName());  // Serializer for keys
        props.put("value.serializer", StringSerializer.class.getName());  // Serializer for values

        // Create the Kafka producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        	
        	
            // Create a message (ProducerRecord)
            String topic = "kafka-test-poc"; // The topic where messages will be sent
            String key = "key1"; // Optional key
            String[] items = {"Abhiav", "Nilima", "Ujjwal", "Minal", "Vaibhav"};// message produce
            Random random = new Random();
            ExecutorService executor = Executors.newSingleThreadExecutor();

            executor.submit(() -> {
                int messageCount = 1;
                while (true) {
                	 String randomItem = items[random.nextInt(items.length)];
                	// Create a ProducerRecord
                	 ProducerRecord<String, String> record = new ProducerRecord<>(topic, String.valueOf(messageCount), randomItem);
                    System.out.println("Producing message: " + messageCount);
                    producer.send(record);
                    messageCount++;
                    try {
                        Thread.sleep(1000); // Simulate processing time (1 second delay)
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        System.out.println("Producer interrupted, shutting down...");
                        break;
                    }
                }
            });

            // Ensure the executor does not shut down (optional)
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("Shutting down producer...");
                executor.shutdown();
            }));
        }
    
	

}
