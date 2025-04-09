package com.example.kafka;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class SimpleKafkaS3 {
	
	public static void main(String[] args) throws IOException {
		Region region = Region.AP_SOUTH_1;
		
		S3Client s3 = S3Client.builder()
                .region(region)
                .credentialsProvider(ProfileCredentialsProvider.create())
                .build();
		
		 listBuckets(s3);
		 
	}
	public static void listBuckets(S3Client s33) throws IOException {
		ListBucketsResponse listBucketsResponse = s33.listBuckets();
		List<Bucket> buckets = listBucketsResponse.buckets();
		System.out.println("S3 Buckets:");
		for (Bucket bucket : buckets) {
			System.out.println(bucket.name());
		}
		
		 String bucketName = "spiro-iot-kafaka-poc";//211125483388
	        S3Client s3 = S3Client.create();

	        // Example key-value pairs
	        storeKeyValue(s33, bucketName, 1, "Hello");
	        storeKeyValue(s33, bucketName, 2, "World");
	}
	
	public static void storeKeyValue(S3Client s33, String bucket, int key, String value) throws IOException {
        String keyStr = String.valueOf(key); // Convert integer key to string

        // Create a temporary file with the value as content
        Path tempFile = Files.createTempFile("s3value", ".txt");
        Files.write(tempFile, value.getBytes(StandardCharsets.UTF_8));

        // Upload the file to S3
        s33.putObject(PutObjectRequest.builder().bucket(bucket).key(keyStr).build(), tempFile);
        System.out.println("Stored key: " + key + ", value: " + value);
    }

}
