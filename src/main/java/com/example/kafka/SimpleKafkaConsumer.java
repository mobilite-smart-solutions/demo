package com.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Properties;

public class SimpleKafkaConsumer {
	private static final String BOOTSTRAP_SERVERS = "localhost:9092";
	

	
	public static void main(String[] args) {
		
	
    String topic = "kafka-test-poc";
    String groupId = "kafka-test";
    String bucketName = "spiro-iot-kafaka-poc";
	Region region = Region.AP_SOUTH_1;
	S3Client s3 = S3Client.builder()
            .region(region)
            .credentialsProvider(ProfileCredentialsProvider.create())
            .build();

    // Configure Kafka consumer properties
    Properties properties = new Properties();
    properties.put("bootstrap.servers", "localhost:9092");
    //properties.put("bootstrap.servers", "65.2.175.140:9094");
    properties.put("group.id", groupId);
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put("auto.offset.reset", "earliest");
    properties.put("offsets.retention.minutes", "1440");

    // Create Kafka consumer
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
    consumer.subscribe(Collections.singletonList(topic));

    // Poll and consume messages
    try {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("Consumed message: key=%s, value=%s, partition=%d, offset=%d%n", 
                                  record.key(), record.value(), record.partition(), record.offset());
                try {
					storeKeyValue(s3, bucketName, 1, record.key());
					storeKeyValue(s3, bucketName, 2, record.value());
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
    	        
            }
        }
    } finally {
        consumer.close();
    }
}
	// Sink to s3
	public static void storeKeyValue(S3Client s3, String bucket, int key, String value) throws IOException {
        String keyStr = String.valueOf(key); // Convert integer key to string

        // Create a temporary file with the value as content
        Path tempFile = Files.createTempFile("s3value", ".txt");
        Files.write(tempFile, value.getBytes(StandardCharsets.UTF_8));

        // Upload the file to S3
        s3.putObject(PutObjectRequest.builder().bucket(bucket).key(keyStr).build(), tempFile);
        System.out.println("File Path"+tempFile.getRoot());
        System.out.println("Stored key: " + key + ", value: " + value);
    }

}
