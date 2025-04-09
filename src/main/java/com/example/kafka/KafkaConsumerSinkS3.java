package com.example.kafka;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;

public class KafkaConsumerSinkS3 {
	 private static final String BOOTSTRAP_SERVERS = "localhost:9092";
	    private static final String GROUP_ID = "s3-consumer-group";
	    private static final String TOPIC = "my-kafka-topic";
	    
	    public static KafkaConsumer<String, String> createConsumer() {
	        Properties props = new Properties();
	        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
	        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
	        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
	        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
	        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

	        return new KafkaConsumer<>(props);
	    }
	
	public static void main(String[] args) {
		
		        KafkaConsumer<String, String> consumer = createConsumer();
		        consumer.subscribe(Collections.singletonList(TOPIC));

		        S3Uploader s3Uploader = new S3Uploader();

		        try {
		            while (true) {
		                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
		                records.forEach(record -> {
		                    System.out.println("Consumed message: " + record.value());
		                    s3Uploader.uploadToS3(BOOTSTRAP_SERVERS);
		                });
		            }
		        } finally {
		            consumer.close();
		        }
		    }
		
	}


