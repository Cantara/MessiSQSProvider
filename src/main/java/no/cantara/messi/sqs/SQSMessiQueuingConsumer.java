package no.cantara.messi.sqs;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import no.cantara.messi.api.MessiClosedException;
import no.cantara.messi.api.MessiQueuingAsyncMessageHandle;
import no.cantara.messi.api.MessiQueuingConsumer;
import no.cantara.messi.api.MessiQueuingMessageHandle;
import no.cantara.messi.protos.MessiMessage;
import no.cantara.messi.protos.MessiProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class SQSMessiQueuingConsumer implements MessiQueuingConsumer {

    private static Logger log = LoggerFactory.getLogger(SQSMessiQueuingConsumer.class);

    final SqsClient sqsClient;
    final String queueNamePrefix;
    final String topic;
    final String queueUrl;
    final AtomicBoolean closed = new AtomicBoolean();

    public SQSMessiQueuingConsumer(SqsClient sqsClient, String queueNamePrefix, String topic, boolean autocreateQueue) {
        this.sqsClient = sqsClient;
        this.queueNamePrefix = queueNamePrefix;
        this.topic = topic;
        String queueName = toQueueName(topic);
        if (autocreateQueue) {
            this.queueUrl = SQSUtils.createQueue(sqsClient, queueName);
        } else {
            this.queueUrl = SQSUtils.getQueueUrl(sqsClient, queueName);
        }
    }

    private String toQueueName(String topic) {
        return queueNamePrefix + topic;
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public MessiQueuingMessageHandle receive(int timeout, TimeUnit timeUnit) throws InterruptedException, MessiClosedException {
        if (closed.get()) {
            throw new MessiClosedException();
        }

        Message message = doReceive(timeout, timeUnit);

        if (message == null) {
            return null;
        }

        MessiMessage messiMessage = toMessiMessage(message);

        return new SQSMessiQueuingMessageHandle(messiMessage, message.receiptHandle(), this::ackMessage);
    }

    private Message doReceive(int timeout, TimeUnit timeUnit) throws InterruptedException {
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

        return message;
    }

    private MessiMessage toMessiMessage(Message message) {
        MessiMessage.Builder builder = MessiMessage.newBuilder();
        try {
            JsonFormat.parser().merge(message.body(), builder);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
        Instant publishedAt = null;
        try {
            if (message.hasMessageAttributes()) {
                MessageAttributeValue publishedAtAttributeValue = message.messageAttributes().get("published_at");
                publishedAt = Instant.parse(publishedAtAttributeValue.stringValue());
            }
        } catch (RuntimeException e) {
            log.warn("Unable to extract publishedAt SQS message-attribute");
        }
        if (!builder.hasFirstProvider()) {
            builder.setFirstProvider(MessiProvider.newBuilder()
                    .setTechnology("SQS")
                    .setPublishedTimestamp(publishedAt == null ? 0 : publishedAt.toEpochMilli())
                    .setSequenceNumber(message.messageId())
                    .build());
        }
        MessiMessage messiMessage = builder
                .setProvider(MessiProvider.newBuilder()
                        .setTechnology("SQS")
                        .setPublishedTimestamp(publishedAt == null ? 0 : publishedAt.toEpochMilli())
                        .setSequenceNumber(message.messageId())
                        .build())
                .build();
        return messiMessage;
    }

    void ackMessage(String receiptHandle) {
        DeleteMessageRequest request = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(receiptHandle)
                .build();
        DeleteMessageResponse response = sqsClient.deleteMessage(request);
        SdkHttpResponse sdkHttpResponse = response.sdkHttpResponse();
        if (!sdkHttpResponse.isSuccessful()) {
            throw new RuntimeException(String.format("Unable to ack/delete SQS message. statusCode=%s, statusText=%s",
                    sdkHttpResponse.statusCode(), sdkHttpResponse.statusText()));
        }
    }

    @Override
    public CompletableFuture<? extends MessiQueuingAsyncMessageHandle> receiveAsync() {
        if (closed.get()) {
            throw new MessiClosedException();
        }
        return CompletableFuture.supplyAsync(() -> {
            try {
                Message message = doReceive(5, TimeUnit.MINUTES);

                if (message == null) {
                    return null;
                }

                MessiMessage messiMessage = toMessiMessage(message);

                return new SQSMessiQueuingAsyncMessageHandle(messiMessage, message.receiptHandle(), this::ackMessage);

            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
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
