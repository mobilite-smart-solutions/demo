package com.example.kafka;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

public class S3Uploader {

    private static final String BUCKET_NAME = "your-s3-bucket-name";
    private final S3Client s3Client;

    public S3Uploader() {
        this.s3Client = S3Client.builder()
                .region(Region.US_WEST_2) // Update with your region
                .credentialsProvider(ProfileCredentialsProvider.create())
                .build();
    }

    public void uploadToS3(String message) {
        String fileName = "kafka-messages/" + System.currentTimeMillis() + ".json";

        PutObjectRequest putRequest = PutObjectRequest.builder()
                .bucket(BUCKET_NAME)
                .key(fileName)
                .build();

        s3Client.putObject(putRequest, 
                software.amazon.awssdk.core.sync.RequestBody.fromBytes(message.getBytes(StandardCharsets.UTF_8)));

        System.out.println("Uploaded to S3: " + fileName);
    }
}
