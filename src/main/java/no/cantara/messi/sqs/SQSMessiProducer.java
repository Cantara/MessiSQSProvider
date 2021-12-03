package no.cantara.messi.sqs;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import no.cantara.messi.api.MessiClosedException;
import no.cantara.messi.api.MessiProducer;
import no.cantara.messi.api.MessiULIDUtils;
import no.cantara.messi.protos.MessiMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.BatchResultErrorEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResultEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public class SQSMessiProducer implements MessiProducer {

    private static Logger log = LoggerFactory.getLogger(SQSMessiProducer.class);

    private final AtomicBoolean closed = new AtomicBoolean();
    private final SqsClient sqsClient;
    private final String queueNamePrefix;
    private final String topic;
    private final String queueUrl;

    public SQSMessiProducer(SqsClient sqsClient, String queueNamePrefix, String topic, boolean autocreateQueue) {
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
    public void publish(MessiMessage... messiMessages) throws MessiClosedException {
        if (closed.get()) {
            throw new MessiClosedException();
        }
        if (messiMessages == null || messiMessages.length == 0) {
            return;
        }

        List<MessiMessage> remainingMessages = new ArrayList<>();
        for (int i = 0; i < messiMessages.length; i++) {
            remainingMessages.add(messiMessages[i]);
        }

        for (int x = 1; remainingMessages.size() > 0; x++) {

            List<SendMessageBatchRequestEntry> entries = new ArrayList<>(messiMessages.length);
            for (int i = 0; i < remainingMessages.size(); i++) {
                MessiMessage messiMessage = remainingMessages.get(i);
                String messiMessageJson;
                try {
                    messiMessageJson = JsonFormat.printer().print(messiMessage);
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                }
                entries.add(SendMessageBatchRequestEntry.builder()
                        .id(String.valueOf(i)) // batch-id
                        .messageBody(messiMessageJson)
                        .build());
            }
            SendMessageBatchRequest sendMessageBatchRequest = SendMessageBatchRequest.builder()
                    .queueUrl(queueUrl)
                    .entries(entries)
                    .build();

            SendMessageBatchResponse response = sqsClient.sendMessageBatch(sendMessageBatchRequest);

            if (!response.sdkHttpResponse().isSuccessful()) {
                throw new RuntimeException(String.format("While putting records to SQS queue '%s'. statusCode=%s, text: '%s'",
                        toQueueName(topic),
                        response.sdkHttpResponse().statusCode(),
                        response.sdkHttpResponse().statusText().orElse("")));
            }

            if (response.hasFailed() && response.failed().size() > 0) {

                /*
                 * At least one message failure
                 */

                List<BatchResultErrorEntry> failed = response.failed();

                int successCount = remainingMessages.size() - failed.size();

                log.warn("Failed to write all records to SQS, potentially re-ordering messages in batch. Attempt={}, successCount={}, failedCount={}", x, successCount, failed.size());

                if (successCount > 0) {
                    /*
                     * At least one message was successfully published to SQS.
                     */

                    if (log.isTraceEnabled()) {
                        List<SendMessageBatchResultEntry> successful = response.successful();
                        for (int j = 0; j < remainingMessages.size(); j++) {
                            MessiMessage message = remainingMessages.get(j);
                            SendMessageBatchResultEntry successEntry = successful.get(j);
                            String messageId = successEntry.messageId();
                            // String sequenceNumber = successEntry.sequenceNumber(); // FIFO-queues onlu
                            log.trace("Sent messages to SQS. ulid={}, messageId={}", MessiULIDUtils.toUlid(message.getUlid()), messageId);
                        }
                    }
                }

                List<MessiMessage> failedMessages = new ArrayList<>(failed.size());

                for (BatchResultErrorEntry failedResultEntry : failed) {
                    int i = Integer.parseInt(failedResultEntry.id());
                    MessiMessage failedMessage = remainingMessages.get(i);
                    failedMessages.add(failedMessage);

                    String errorCode = failedResultEntry.code();
                    if (errorCode != null) {
                        if (log.isDebugEnabled()) {
                            String errorMessage = failedResultEntry.message();
                            log.trace("Message with ulid={} write to SQS failed. errorCode={}, errorMessage: {}", MessiULIDUtils.toUlid(failedMessage.getUlid()), errorCode, errorMessage);
                        }
                    }

                    if (failedResultEntry.senderFault()) {
                        String errorMessage = failedResultEntry.message();
                        throw new IllegalArgumentException(String.format("Sender fault error from SQS server. code='%s', errorMessage='%s'. Message: %s", errorCode, errorMessage, failedMessage.toString()));
                    }
                }

                remainingMessages = failedMessages;

            } else {

                /*
                 * All messages were successfully published to SQS.
                 */

                if (log.isTraceEnabled()) {
                    List<SendMessageBatchResultEntry> successful = response.successful();
                    for (int j = 0; j < successful.size(); j++) {
                        SendMessageBatchResultEntry successEntry = successful.get(j);
                        MessiMessage message = remainingMessages.get(Integer.parseInt(successEntry.id()));
                        String messageId = successEntry.messageId();
                        // String sequenceNumber = successEntry.sequenceNumber(); // FIFO-queues onlu
                        log.trace("Sent messages to SQS. ulid={}, messageId={}", MessiULIDUtils.toUlid(message.getUlid()), messageId);
                    }
                }

                remainingMessages.clear();
            }
        }
    }

    @Override
    public CompletableFuture<Void> publishAsync(MessiMessage... messiMessages) {
        if (closed.get()) {
            throw new MessiClosedException();
        }
        return CompletableFuture.supplyAsync(() -> {
            publish(messiMessages);
            return null;
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
