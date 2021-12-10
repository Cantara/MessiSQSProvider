package no.cantara.messi.sqs;

import no.cantara.messi.api.MessiClient;
import no.cantara.messi.api.MessiClosedException;
import no.cantara.messi.api.MessiCursor;
import no.cantara.messi.api.MessiMetadataClient;
import no.cantara.messi.api.MessiTopic;
import no.cantara.messi.protos.MessiMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class SQSMessiClient implements MessiClient {

    private static Logger log = LoggerFactory.getLogger(SQSMessiClient.class);

    final SqsClient sqsClient;
    final String queueNamePrefix;
    final boolean autocreateQueue;

    final AtomicBoolean closed = new AtomicBoolean();

    final Map<String, SQSMessiTopic> topicByName = new ConcurrentHashMap<>();

    public SQSMessiClient(SqsClient sqsClient, String queueNamePrefix, boolean autocreateQueue) {
        this.sqsClient = sqsClient;
        this.queueNamePrefix = queueNamePrefix;
        this.autocreateQueue = autocreateQueue;
    }

    @Override
    public MessiTopic topicOf(String name) {
        return topicByName.computeIfAbsent(name, topicName -> new SQSMessiTopic(this, topicName, sqsClient, queueNamePrefix, autocreateQueue));
    }

    @Override
    public MessiCursor.Builder cursorOf() {
        throw new UnsupportedOperationException();
    }

    @Override
    public MessiMessage lastMessage(String topic, String shardId) throws MessiClosedException {
        throw new UnsupportedOperationException();
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
        for (SQSMessiTopic topic : topicByName.values()) {
            topic.close();
        }
        topicByName.clear();
        sqsClient.close();
    }

    @Override
    public MessiMetadataClient metadata(String topic) {
        throw new UnsupportedOperationException("SQS provider does not support metadata-client");
    }
}
