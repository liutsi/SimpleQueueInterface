package com.example;

import org.apache.http.util.Asserts;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class AmazonSQSQueueTest {
    //
    // Implement me.
    //
    @Before
    public void setup() {

        System.setProperty("QUEUE_TYPE", "AWS");
    }

    @Test
    public void pushShouldReturnTrue() {
        QueueService queueService = QueueFactory.getQueueService();
        boolean result = queueService.push("q1", "m1");
        Asserts.check(result, "push should return true");
    }

    @Test
    public void pushThenPullShouldReturnQueueMessage1() {
        QueueService queueService = QueueFactory.getQueueService();
        String queueName = "q1test";
        queueService.clear(queueName);
        String testcontent = "m1";
        boolean result = queueService.push(queueName, testcontent);
        QueueService.QueueMessage qm = queueService.pull(queueName);
        Asserts.check(qm.getContent().equals(testcontent), "returned msg id is not 1, it's " + qm.getId());
    }

    @Test
    public void push1000MultiThreadShouldReturn1000() {
        Long startTime = System.currentTimeMillis();
        QueueService queueService = QueueFactory.getQueueService();
        ExecutorService executorService = Executors.newFixedThreadPool(3);
        List<Future> task = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            task.add(executorService.submit(new Runnable() {
                @Override public void run() {
                    AtomicInteger i = new AtomicInteger();
                    for (int j = 0; j < 7; j++) {
                        queueService.push("q1", "message" + Thread.currentThread().getId() + "m" + i.incrementAndGet());
                    }
                }
            }));
        }
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                System.err.println("Threads didn't finish in 60000 seconds!");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        Asserts.check(queueService.size("q1") == 20000, "multithread fail, size is " + queueService.size("q1"));

        System.out.println("This test takes time: " + (System.currentTimeMillis() - startTime) + "ms");
    }

    @Test
    public void pull5msgs() {
        QueueService queueService = QueueFactory.getQueueService();
        String queueName = "q1";
        queueService.clear(queueName);
        String testcontent = "m1";
        //        boolean result = queueService.push(queueName, testcontent);
        for (int i = 0; i < 15; i++) {
            QueueService.QueueMessage qm = queueService.pull(queueName);
            if (qm != null)
                System.out.println(qm.getContent().toString());
        }
    }
}
