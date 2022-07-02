package com.example;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class InMemoryQueueService<E> implements QueueService<E> {
    private Map<String, Queue<QueueMessage>> queueName2list = new ConcurrentHashMap<>();

    protected InMemoryQueueService() {
    }

    @Override public boolean push(final String queueUrl, final E content) {
        Queue<QueueMessage> queue = getQueue(queueUrl);
        return queue.offer(new QueueMessage(queueUrl, String.valueOf(queue.size()), content));
    }

    private synchronized Queue<QueueMessage> getQueue(final String queueUrl) {
        Queue<QueueMessage> queue = queueName2list.get(queueUrl);
        if (queue == null) {
            if (queue == null) {
                queue = new ConcurrentLinkedQueue<>();
                queueName2list.put(queueUrl, queue);
            }
        }
        return queue;
    }

    @Override public QueueMessage pull(final String queueUrl) {
        Queue<QueueMessage> queue = getQueue(queueUrl);
        return queue.poll();
    }

    @Override public QueueMessage delete(final String queueUrl, final String messageId) {
        Queue<QueueMessage> queue = getQueue(queueUrl);
        return queue.remove();
    }

    @Override public int size(final String queueUrl) {
        Queue<QueueMessage> queue = getQueue(queueUrl);
        return queue.size();
    }

    @Override public void clear(String queueUrl) {
        Queue<QueueMessage> queue = getQueue(queueUrl);
        queue.clear();
    }
}
