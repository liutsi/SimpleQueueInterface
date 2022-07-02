package com.example;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.AmazonSQSException;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.amazonaws.util.CollectionUtils;

import java.util.Collections;
import java.util.List;

public class SqsQueueService<E> implements QueueService<E> {
  //
  // Task 4: Optionally implement parts of me.
  //
  // This file is a placeholder for an AWS-backed implementation of QueueService.  It is included
  // primarily so you can quickly assess your choices for method signatures in QueueService in
  // terms of how well they map to the implementation intended for a production environment.
  //
  AmazonSQS sqsClient = null;

  public SqsQueueService(AmazonSQS sqsClient) {
    this.sqsClient = sqsClient;
  }

  private String createQueueIfNotExist(String QUEUE_NAME) {
    String queueUrl = null;
    try {
      queueUrl = sqsClient.getQueueUrl(QUEUE_NAME).getQueueUrl();
    } catch (Exception e) {
      e.printStackTrace();
    }
    if (queueUrl != null)
      return queueUrl;

    CreateQueueRequest create_request = new CreateQueueRequest(QUEUE_NAME)
                    .addAttributesEntry("DelaySeconds", "60")
                    .addAttributesEntry("MessageRetentionPeriod", "86400");

    try {
      CreateQueueResult result = sqsClient.createQueue(create_request);
      return result.getQueueUrl();
    } catch (AmazonSQSException e) {
      if (!e.getErrorCode().equals("QueueAlreadyExists")) {
        throw e;
      }
    }
    return null;
  }

  private String getQueueUrl(String queueName) {
    return createQueueIfNotExist(queueName);
  }

  @Override public boolean push(final String queueUrl, final E content) {
    SendMessageRequest send_msg_request = new SendMessageRequest()
                    .withQueueUrl(getQueueUrl(queueUrl))
                    .withMessageBody(content.toString())
                    .withDelaySeconds(5);
    SendMessageResult result = sqsClient.sendMessage(send_msg_request);

    return result.getMessageId() != null;
  }

  @Override public QueueMessage pull(final String queueUrl) {
    String queueUrlReal = getQueueUrl(queueUrl);
    ReceiveMessageResult result = sqsClient.receiveMessage(queueUrlReal);
    List<Message> messages = result.getMessages();
    if (CollectionUtils.isNullOrEmpty(messages)) {
      return null;
    }
    Message message = messages.get(0);
    String msgbody = message.getBody();
    sqsClient.deleteMessage(queueUrlReal, message.getReceiptHandle());
    return new QueueMessage(queueUrl, message.getMessageId(), msgbody);
  }

  @Override public QueueMessage delete(final String queueUrl, final String messageId) {
    return null;
  }

  @Override public int size(final String queueUrl) {
    return 0;
  }

  @Override public void clear(final String queueUrl) {

  }
}
