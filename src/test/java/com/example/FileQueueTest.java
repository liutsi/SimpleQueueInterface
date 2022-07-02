package com.example;

import org.apache.http.util.Asserts;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class FileQueueTest {

    @Test
    public void singleMsgWrite() {
        System.setProperty("QUEUE_TYPE", "FILE");

        QueueService queueService = QueueFactory.getQueueService();
        boolean result = queueService.push("q1", "m1");
        Asserts.check(result, "push should return true");
    }

    @Test
    public void singleMsgWriteThenPull() {
        System.setProperty("QUEUE_TYPE", "FILE");

        QueueService queueService = QueueFactory.getQueueService();
        String queueUrl = "q1";
        queueService.clear(queueUrl);
        String testcontent = "m2";
        boolean result = queueService.push(queueUrl, testcontent);
        QueueService.QueueMessage msg = queueService.pull(queueUrl);
        Asserts.check(msg.getContent().equals(testcontent), "push should return true");
    }

    @Test
    public void singleMsgWriteThenPullTwice() {
        System.setProperty("QUEUE_TYPE", "FILE");

        QueueService queueService = QueueFactory.getQueueService();
        String queueUrl = "q1";
        queueService.clear(queueUrl);
        String testcontent1 = "m1";
        String testcontent2 = "m2";
        boolean result = queueService.push(queueUrl, testcontent1);
        result = queueService.push(queueUrl, testcontent2);
        QueueService.QueueMessage msg1 = queueService.pull(queueUrl);
        QueueService.QueueMessage msg2 = queueService.pull(queueUrl);
        Asserts.check(msg1.getContent().equals(testcontent1) && msg2.getContent().equals(testcontent2),
                        "what we pulled is not what we pushed");
    }

    @Test
    public void push1000MultiThreadShouldReturn1000() {
        System.setProperty("QUEUE_TYPE", "FILE");
        QueueService queueService = QueueFactory.getQueueService();
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        List<Future> task = new ArrayList<>();

        for (int i = 0; i < 15; i++) {
            task.add(executorService.submit(new Runnable() {
                @Override public void run() {
                    AtomicInteger i = new AtomicInteger();
                    String queueName = "que" + String.valueOf(Math.ceil(1000 * Math.random()));
                    for (int j = 0; j < 50; j++) {
                        queueService.push(queueName,
                                        "m" + i.incrementAndGet());
                    }
                }
            }));
        }
        try {
            executorService.shutdown();
            executorService.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        //        Asserts.check(queueService.size(queueName) == 1558, "multithread fail, size is " + queueService.size(queueName));

    }
}
