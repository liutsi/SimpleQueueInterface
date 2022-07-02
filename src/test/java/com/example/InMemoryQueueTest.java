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

public class InMemoryQueueTest {
    //
    // Implement me.
    //
    @Test
    public void pushShouldReturnTrue() {
        System.setProperty("QUEUE_TYPE", "INMEMORY");
        QueueService queueService = QueueFactory.getQueueService();
        boolean result = queueService.push("q1", "m1");
        Asserts.check(result, "push should return true");
    }

    @Test
    public void pushThenPullShouldReturnQueueMessage1() {
        System.setProperty("QUEUE_TYPE", "INMEMORY");
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
        System.setProperty("QUEUE_TYPE", "INMEMORY");
        Long startTime = System.currentTimeMillis();
        QueueService queueService = QueueFactory.getQueueService();
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        List<Future> task = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
            task.add(executorService.submit(new Runnable() {
                @Override public void run() {
                    AtomicInteger i = new AtomicInteger();
                    for (int j = 0; j < 500; j++) {
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

        System.out.println("This test takes time: " + (System.currentTimeMillis() - startTime)  +"ms");
    }
}
