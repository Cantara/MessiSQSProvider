package no.cantara.messi.sqs;

import no.cantara.messi.api.MessiClosedException;
import no.cantara.messi.api.MessiCursor;
import no.cantara.messi.api.MessiMetadataClient;
import no.cantara.messi.api.MessiProducer;
import no.cantara.messi.api.MessiQueuingConsumer;
import no.cantara.messi.api.MessiShard;
import no.cantara.messi.api.MessiTopic;
import no.cantara.messi.protos.MessiMessage;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

public class SQSMessiTopic implements MessiTopic {

    final SQSMessiClient messiClient;
    final String name;
    final SqsClient sqsClient;
    final String queueNamePrefix;
    final boolean autocreateQueue;

    final AtomicBoolean closed = new AtomicBoolean();
    final CopyOnWriteArrayList<SQSMessiProducer> producers = new CopyOnWriteArrayList<>();
    final CopyOnWriteArrayList<SQSMessiQueuingConsumer> consumers = new CopyOnWriteArrayList<>();
    final MessiShard theShard = new SQSMessiShard();

    public SQSMessiTopic(SQSMessiClient messiClient, String name, SqsClient sqsClient, String queueNamePrefix, boolean autocreateQueue) {
        this.messiClient = messiClient;
        this.name = name;
        this.sqsClient = sqsClient;
        this.queueNamePrefix = queueNamePrefix;
        this.autocreateQueue = autocreateQueue;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public MessiProducer producer() {
        if (closed.get()) {
            throw new MessiClosedException();
        }
        SQSMessiProducer producer = new SQSMessiProducer(sqsClient, queueNamePrefix, name, autocreateQueue);
        producers.add(producer);
        return producer;
    }

    @Override
    public MessiShard shardOf(String shardId) {
        return theShard;
    }

    @Override
    public MessiMetadataClient metadata() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SQSMessiClient client() {
        return messiClient;
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        closed.set(true);
        for (SQSMessiQueuingConsumer consumer : consumers) {
            consumer.close();
        }
        consumers.clear();
        for (SQSMessiProducer producer : producers) {
            producer.close();
        }
        producers.clear();
    }

    public class SQSMessiShard implements MessiShard {

        @Override
        public boolean supportsQueuing() {
            return true;
        }

        @Override
        public MessiQueuingConsumer queuingConsumer() {
            SQSMessiQueuingConsumer consumer = new SQSMessiQueuingConsumer(this, sqsClient, queueNamePrefix, name, autocreateQueue);
            consumers.add(consumer);
            return consumer;
        }

        @Override
        public MessiCursor.Builder cursorOf() {
            throw new UnsupportedOperationException();
        }

        @Override
        public MessiCursor cursorOfCheckpoint(String checkpoint) {
            throw new UnsupportedOperationException();
        }

        @Override
        public MessiCursor cursorAt(MessiMessage message) {
            throw new UnsupportedOperationException();
        }

        @Override
        public MessiCursor cursorAfter(MessiMessage message) {
            throw new UnsupportedOperationException();
        }

        @Override
        public MessiCursor cursorAtLastMessage() throws MessiClosedException {
            throw new UnsupportedOperationException();
        }

        @Override
        public MessiCursor cursorAfterLastMessage() throws MessiClosedException {
            throw new UnsupportedOperationException();
        }

        @Override
        public MessiCursor cursorHead() {
            throw new UnsupportedOperationException();
        }

        @Override
        public MessiCursor cursorAtTrimHorizon() {
            throw new UnsupportedOperationException();
        }

        @Override
        public SQSMessiTopic topic() {
            return SQSMessiTopic.this;
        }

        @Override
        public void close() {
        }
    }
}
