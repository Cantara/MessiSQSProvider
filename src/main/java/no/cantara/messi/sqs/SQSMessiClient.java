package no.cantara.messi.sqs;

import no.cantara.messi.api.MessiClient;
import no.cantara.messi.api.MessiClosedException;
import no.cantara.messi.api.MessiCursor;
import no.cantara.messi.api.MessiMetadataClient;
import no.cantara.messi.protos.MessiMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

public class SQSMessiClient implements MessiClient {

    private static Logger log = LoggerFactory.getLogger(SQSMessiClient.class);

    final SqsClient sqsClient;
    final String queueNamePrefix;
    final boolean autocreateQueue;

    final AtomicBoolean closed = new AtomicBoolean();
    final CopyOnWriteArrayList<SQSMessiConsumer> consumers = new CopyOnWriteArrayList<>();
    final CopyOnWriteArrayList<SQSMessiProducer> producers = new CopyOnWriteArrayList<>();

    public SQSMessiClient(SqsClient sqsClient, String queueNamePrefix, boolean autocreateQueue) {
        this.sqsClient = sqsClient;
        this.queueNamePrefix = queueNamePrefix;
        this.autocreateQueue = autocreateQueue;
    }

    @Override
    public SQSMessiProducer producer(String topic) {
        if (closed.get()) {
            throw new MessiClosedException();
        }
        SQSMessiProducer producer = new SQSMessiProducer(sqsClient, queueNamePrefix, topic, autocreateQueue);
        producers.add(producer);
        return producer;
    }

    @Override
    public SQSMessiConsumer consumer(String topic, MessiCursor cursor) {
        if (closed.get()) {
            throw new MessiClosedException();
        }
        SQSMessiConsumer consumer = new SQSMessiConsumer(sqsClient, queueNamePrefix, topic, autocreateQueue);
        consumers.add(consumer);
        return consumer;
    }

    @Override
    public SQSMessiCursor.Builder cursorOf() {
        if (closed.get()) {
            throw new MessiClosedException();
        }
        return new SQSMessiCursor.Builder();
    }

    @Override
    public MessiMessage lastMessage(String topic, String shardId) throws MessiClosedException {
        if (closed.get()) {
            throw new MessiClosedException();
        }
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public List<String> shards(String topic) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        closed.set(true);
        try {
            for (SQSMessiConsumer consumer : consumers) {
                consumer.close();
            }
            consumers.clear();
            for (SQSMessiProducer producer : producers) {
                producer.close();
            }
            producers.clear();
        } finally {
            sqsClient.close();
        }
    }

    @Override
    public MessiMetadataClient metadata(String topic) {
        if (closed.get()) {
            throw new MessiClosedException();
        }
        throw new UnsupportedOperationException("SQS provider does not support metadata-client");
    }
}
