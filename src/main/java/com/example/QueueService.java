package com.example;

public interface QueueService<E> {

    //
    // Task 1: Define me.
    //
    // This interface should include the following methods.  You should choose appropriate
    // signatures for these methods that prioritise simplicity of implementation for the range of
    // intended implementations (in-memory, file, and SQS).  You may include additional methods if
    // you choose.
    //
    boolean push(String queueUrl, E content);

    //   pushes a message onto a queue.
    QueueMessage pull(String queueUrl);

    //   retrieves a single message from a queue.
    QueueMessage delete(String queueUrl, String messageId);

    //   deletes a message from the queue that was received by pull().
    //
    int size(String queueUrl);

    void clear(String queueUrl);

    class QueueMessage<E> {
        public QueueMessage(final String queueUrl, final String id, final E content) {
            this.queueUrl = queueUrl;
            this.id = id;
            this.content = content;
        }

        @Override public String toString() {
            return queueUrl + "," + String.valueOf(id) + "," + content+"\n";
        }

        private String queueUrl;
        private String id;
        private E content;

        public String getQueueUrl() {
            return queueUrl;
        }

        public QueueMessage<E> setQueueUrl(final String queueUrl) {
            this.queueUrl = queueUrl;
            return this;
        }

        public String getId() {
            return id;
        }

        public QueueMessage setId(final String id) {
            this.id = id;
            return this;
        }

        public E getContent() {
            return content;
        }

        public QueueMessage setContent(final E content) {
            this.content = content;
            return this;
        }
    }

}
