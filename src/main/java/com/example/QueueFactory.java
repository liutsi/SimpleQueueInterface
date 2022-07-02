package com.example;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;

public class QueueFactory {
    private static QueueService serviceInMemory;
    private static QueueService serviceFile;
    private static QueueService serviceAws;

    public static QueueService getQueueService() {
        synchronized (QueueFactory.class) {
            String type = System.getProperty("QUEUE_TYPE", "AWS");
            switch (type) {
                case "AWS": {
                    if (serviceAws == null) {
                        AWSCredentials credentials = new BasicAWSCredentials(
                                        "AKIARVL6CBIBR7HZQWRS",
                                        "95yin0r+61gkO78yoOTTjSkIwu30pqjyDwqgakUB"
                        );
                        AmazonSQS sqs = AmazonSQSClientBuilder.standard()
                                        .withCredentials(new AWSStaticCredentialsProvider(credentials))
                                        .withRegion(Regions.AP_SOUTHEAST_2)
                                        .build();
                        serviceAws = new SqsQueueService(sqs);
                    }
                    return serviceAws;
                }
                case "INMEMORY": {
                    if (serviceInMemory == null)
                        serviceInMemory = new InMemoryQueueService();
                    return serviceInMemory;
                }
                case "FILE": {
                    if (serviceFile == null)
                        serviceFile = new FileQueueService();
                    return serviceFile;
                }
            }
        }
        return null;
    }
}
