package com.example;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.Thread.sleep;

public class FileQueueService<E> implements QueueService<E> {
    private Path rootPath = Path.of(System.getProperty("user.dir"), "filequeue");

    private int msgId = 0;

    public FileQueueService() {
    }

    @Override public boolean push(final String queueUrl, final E content) {
        Path queueFolder = rootPath.resolve(queueUrl);
        lock(queueFolder);

        try {
            Files.writeString(queueFolder.resolve("content"),
                            new QueueMessage<E>(queueUrl, String.valueOf(++msgId), content).toString(),
                            Charset.forName("utf-16"),
                            StandardOpenOption.APPEND, StandardOpenOption.CREATE);
            return true;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            unlock(queueFolder);
        }
    }

    @Override public QueueMessage pull(final String queueUrl) {
        Path queueFolder = rootPath.resolve(queueUrl);
        lock(queueFolder);

        try {
            String line = Files.readString(queueFolder.resolve("content"), StandardCharsets.UTF_16);
            int firstLineEnd = line.indexOf("\n");

            String[] splits = line.substring(0, firstLineEnd).split(",");

            String remainder = line.substring(firstLineEnd+1);
            Files.writeString(queueFolder.resolve("content"), remainder, StandardCharsets.UTF_16, StandardOpenOption.TRUNCATE_EXISTING);

            return new QueueMessage(splits[0], splits[1], splits[2]);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            unlock(queueFolder);
        }
    }

    @Override public QueueMessage delete(final String queueUrl, final String messageId) {
        return null;
    }

    @Override public int size(final String queueUrl) {
        Path queueFolder = rootPath.resolve(queueUrl);
        lock(queueFolder);

        try {
            List<String> list = Files.readAllLines(queueFolder.resolve("content"),
                            Charset.forName("utf-16"));
            return list.size();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            unlock(queueFolder);
        }
    }

    @Override public void clear(String queueUrl) {
        Path queueFolder = rootPath.resolve(queueUrl);
        lock(queueFolder);

        try {
            Files.writeString(queueFolder.resolve("content"),
                            "",
                            Charset.forName("utf-16"),
                            StandardOpenOption.TRUNCATE_EXISTING); //clear all content

        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            unlock(queueFolder);
        }
    }

    private boolean lock(Path queueFolder) {
        File lockfile = queueFolder.resolve(".lock").toFile();
        while (!lockfile.mkdirs()) {
            try {
                sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        return true;
    }
    private boolean unlock(Path queueFolder) {
        File lockfile = queueFolder.resolve(".lock").toFile();
        lockfile.delete();
        return true;
    }
}
