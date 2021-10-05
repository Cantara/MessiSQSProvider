package no.cantara.messi.sqs;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import no.cantara.messi.api.MessiClosedException;
import no.cantara.messi.api.MessiConsumer;
import no.cantara.messi.protos.MessiMessage;
import no.cantara.messi.protos.MessiProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class SQSMessiConsumer implements MessiConsumer {

    private static Logger log = LoggerFactory.getLogger(SQSMessiConsumer.class);

    final SqsClient sqsClient;
    final String queueNamePrefix;
    final String topic;
    final String queueUrl;
    final AtomicBoolean closed = new AtomicBoolean();

    public SQSMessiConsumer(SqsClient sqsClient, String queueNamePrefix, String topic) {
        this.sqsClient = sqsClient;
        this.queueNamePrefix = queueNamePrefix;
        this.topic = topic;
        this.queueUrl = SQSUtils.createQueue(sqsClient, toQueueName(topic));
    }

    private String toQueueName(String topic) {
        return queueNamePrefix + "/" + topic;
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public MessiMessage receive(int timeout, TimeUnit timeUnit) throws InterruptedException, MessiClosedException {
        if (closed.get()) {
            throw new MessiClosedException();
        }

        long startTime = System.currentTimeMillis();

        int waitTimeSeconds = (int) timeUnit.toSeconds(timeout);

        List<Message> messages;
        do {

            int timeLeftSeconds = Math.max(1, (int) Math.round(((startTime + (1000L * waitTimeSeconds)) - System.currentTimeMillis()) / 1000.0));

            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(1)
                    .waitTimeSeconds(Math.min(timeLeftSeconds, 5)) // long-polling
                    .build();

            ReceiveMessageResponse receiveMessageResponse = sqsClient.receiveMessage(receiveMessageRequest);
            SdkHttpResponse receiveMessageSdkHttpResponse = receiveMessageResponse.sdkHttpResponse();
            if (!receiveMessageSdkHttpResponse.isSuccessful()) {
                throw new RuntimeException(String.format("Unable to receive SQS message. statusCode=%s, statusText=%s",
                        receiveMessageSdkHttpResponse.statusCode(), receiveMessageSdkHttpResponse.statusText()));
            }

            messages = receiveMessageResponse.messages();

        } while (messages.isEmpty() && System.currentTimeMillis() < (startTime + (1000L * waitTimeSeconds)));

        if (messages.isEmpty()) {
            return null;
        }

        Message message = messages.get(0);

        MessiMessage.Builder builder = MessiMessage.newBuilder();
        try {
            JsonFormat.parser().merge(message.body(), builder);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
        MessiMessage messiMessage = builder
                .setProvider(MessiProvider.newBuilder()
                        .setSequenceNumber(message.messageId())
                        .build())
                .build();

        ackMessage(message);

        return messiMessage;
    }

    void ackMessage(Message message) {
        DeleteMessageRequest request = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle())
                .build();
        DeleteMessageResponse response = sqsClient.deleteMessage(request);
        SdkHttpResponse sdkHttpResponse = response.sdkHttpResponse();
        if (!sdkHttpResponse.isSuccessful()) {
            throw new RuntimeException(String.format("Unable to ack/delete SQS message. statusCode=%s, statusText=%s",
                    sdkHttpResponse.statusCode(), sdkHttpResponse.statusText()));
        }
    }

    @Override
    public CompletableFuture<? extends MessiMessage> receiveAsync() {
        if (closed.get()) {
            throw new MessiClosedException();
        }
        return CompletableFuture.supplyAsync(() -> {
            try {
                return receive(5, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void seek(long timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SQSMessiCursor cursorAt(MessiMessage messiMessage) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SQSMessiCursor cursorAfter(MessiMessage messiMessage) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        closed.set(true);
    }
}
