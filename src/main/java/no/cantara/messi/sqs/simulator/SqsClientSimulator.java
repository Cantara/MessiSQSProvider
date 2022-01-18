package no.cantara.messi.sqs.simulator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.BatchEntryIdsNotDistinctException;
import software.amazon.awssdk.services.sqs.model.BatchRequestTooLongException;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageResponse;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueResponse;
import software.amazon.awssdk.services.sqs.model.EmptyBatchRequestException;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesResponse;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.InvalidAttributeNameException;
import software.amazon.awssdk.services.sqs.model.InvalidBatchEntryIdException;
import software.amazon.awssdk.services.sqs.model.InvalidIdFormatException;
import software.amazon.awssdk.services.sqs.model.ListQueuesRequest;
import software.amazon.awssdk.services.sqs.model.ListQueuesResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;
import software.amazon.awssdk.services.sqs.model.OverLimitException;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;
import software.amazon.awssdk.services.sqs.model.ReceiptHandleIsInvalidException;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResultEntry;
import software.amazon.awssdk.services.sqs.model.SqsException;
import software.amazon.awssdk.services.sqs.model.TooManyEntriesInBatchRequestException;
import software.amazon.awssdk.services.sqs.model.UnsupportedOperationException;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class SqsClientSimulator implements SqsClient {

    static class SimulatedSqsQueue {
        final String url;
        final String name;
        final AtomicLong nextMessageId = new AtomicLong(1);
        final ConcurrentNavigableMap<Long, Message> primary = new ConcurrentSkipListMap<>();
        final ConcurrentNavigableMap<Long, Message> delivered = new ConcurrentSkipListMap<>();

        SimulatedSqsQueue(String url, String name) {
            this.url = url;
            this.name = name;
        }

        public void close() {
            primary.clear();
            delivered.clear();
        }
    }

    private static Logger log = LoggerFactory.getLogger(SqsClientSimulator.class);

    final Map<String, SimulatedSqsQueue> queueByUrl = new ConcurrentHashMap<>();

    @Override
    public String serviceName() {
        return SqsClient.SERVICE_NAME;
    }

    @Override
    public void close() {
        for (Map.Entry<String, SimulatedSqsQueue> entry : queueByUrl.entrySet()) {
            entry.getValue().close();
        }
        queueByUrl.clear();
    }

    @Override
    public CreateQueueResponse createQueue(CreateQueueRequest createQueueRequest) {
        String queueUrl = toQueueUrl(createQueueRequest.queueName());
        SimulatedSqsQueue queue = queueByUrl.computeIfAbsent(queueUrl, url -> new SimulatedSqsQueue(url, createQueueRequest.queueName()));
        CreateQueueResponse.Builder responseBuilder = CreateQueueResponse.builder();
        responseBuilder.sdkHttpResponse(SdkHttpResponse.builder()
                .statusCode(200)
                .build());
        return responseBuilder
                .queueUrl(queue.url)
                .build();
    }

    String toQueueUrl(String queueName) {
        return "messi-sqs-simulator://" + queueName;
    }

    String toQueueName(String queueUrl) {
        return queueUrl.substring("messi-sqs-simulator://".length());
    }

    String toQueueArn(String queueUrl) {
        return "arn:aws-simulator:messi-sqs-simulator:region-x:123456789012:" + toQueueName(queueUrl);
    }

    @Override
    public ListQueuesResponse listQueues(ListQueuesRequest listQueuesRequest) throws AwsServiceException, SdkClientException, SqsException {
        ListQueuesResponse.Builder responseBuilder = ListQueuesResponse.builder();
        responseBuilder.sdkHttpResponse(SdkHttpResponse.builder()
                .statusCode(200)
                .build());
        return responseBuilder
                .queueUrls(queueByUrl.keySet())
                .build();
    }

    @Override
    public GetQueueUrlResponse getQueueUrl(GetQueueUrlRequest getQueueUrlRequest) throws QueueDoesNotExistException, AwsServiceException, SdkClientException, SqsException {
        GetQueueUrlResponse.Builder responseBuilder = GetQueueUrlResponse.builder();
        responseBuilder.sdkHttpResponse(SdkHttpResponse.builder()
                .statusCode(200)
                .build());
        String queueUrl = toQueueUrl(getQueueUrlRequest.queueName());
        if (queueByUrl.containsKey(queueUrl)) {
            return responseBuilder
                    .queueUrl(queueUrl)
                    .build();
        } else {
            throw QueueDoesNotExistException.builder()
                    .statusCode(400)
                    .message("Simulated queue does not exist")
                    .build();
        }
    }

    private final Set<String> allowedAttributeNames = new LinkedHashSet<>();

    {
        allowedAttributeNames.add(QueueAttributeName.QUEUE_ARN.toString());
    }

    @Override
    public GetQueueAttributesResponse getQueueAttributes(GetQueueAttributesRequest getQueueAttributesRequest) throws InvalidAttributeNameException, AwsServiceException, SdkClientException, SqsException {
        GetQueueAttributesResponse.Builder responseBuilder = GetQueueAttributesResponse.builder();
        responseBuilder.sdkHttpResponse(SdkHttpResponse.builder()
                .statusCode(200)
                .build());
        String queueUrl = getQueueAttributesRequest.queueUrl();
        if (queueByUrl.containsKey(queueUrl)) {
            List<String> attributeNames = getQueueAttributesRequest.attributeNamesAsStrings();
            for (String requestedAttributeName : attributeNames) {
                if (!allowedAttributeNames.contains(requestedAttributeName)) {
                    throw InvalidAttributeNameException.builder().message("Attribute not supported: " + requestedAttributeName).build();
                }
            }
            Map<QueueAttributeName, String> attributes = new LinkedHashMap<>();
            if (attributeNames.contains(QueueAttributeName.QUEUE_ARN.toString())) {
                String queueArn = toQueueArn(queueUrl);
                attributes.put(QueueAttributeName.QUEUE_ARN, queueArn);
            }
            return responseBuilder
                    .attributes(attributes)
                    .build();
        } else {
            throw QueueDoesNotExistException.builder()
                    .statusCode(400)
                    .message("Simulated queue does not exist")
                    .build();
        }
    }

    @Override
    public DeleteQueueResponse deleteQueue(DeleteQueueRequest deleteQueueRequest) throws AwsServiceException, SdkClientException, SqsException {
        SimulatedSqsQueue queue = queueByUrl.remove(deleteQueueRequest.queueUrl());
        if (queue != null) {
            queue.close();
        }
        DeleteQueueResponse.Builder responseBuilder = DeleteQueueResponse.builder();
        responseBuilder.sdkHttpResponse(SdkHttpResponse.builder()
                .statusCode(200)
                .build());
        return responseBuilder
                .build();
    }

    @Override
    public SendMessageBatchResponse sendMessageBatch(SendMessageBatchRequest sendMessageBatchRequest) throws TooManyEntriesInBatchRequestException, EmptyBatchRequestException, BatchEntryIdsNotDistinctException, BatchRequestTooLongException, InvalidBatchEntryIdException, UnsupportedOperationException, AwsServiceException, SdkClientException, SqsException {
        SimulatedSqsQueue queue = queueByUrl.get(sendMessageBatchRequest.queueUrl());
        List<SendMessageBatchRequestEntry> entries = sendMessageBatchRequest.entries();
        List<SendMessageBatchResultEntry> successfulEntries = new ArrayList<>();
        for (SendMessageBatchRequestEntry entry : entries) {
            Message.Builder builder = Message.builder();
            if (entry.hasMessageAttributes()) {
                builder.messageAttributes(entry.messageAttributes());
            }
            if (entry.hasMessageSystemAttributes()) {
                Map<MessageSystemAttributeName, String> systemAttributes = entry.messageSystemAttributes().entrySet().stream()
                        .filter(e -> MessageSystemAttributeName.UNKNOWN_TO_SDK_VERSION != MessageSystemAttributeName.valueOf(e.getKey().toString()))
                        .collect(Collectors.toMap(
                                e -> MessageSystemAttributeName.valueOf(e.getKey().toString()),
                                e -> e.getValue().toString())
                        );
                builder.attributes(systemAttributes);
            }
            Long messageId = queue.nextMessageId.getAndIncrement();
            String messageIdStr = String.valueOf(messageId);
            Message message = builder
                    .messageId(messageIdStr)
                    .receiptHandle(messageIdStr)
                    .body(entry.messageBody())
                    .build();
            queue.primary.put(messageId, message);
            successfulEntries.add(SendMessageBatchResultEntry.builder()
                    .messageId(messageIdStr)
                    .build());
        }
        SendMessageBatchResponse.Builder responseBuilder = SendMessageBatchResponse.builder();
        responseBuilder.sdkHttpResponse(SdkHttpResponse.builder()
                .statusCode(200)
                .build());
        responseBuilder.successful(successfulEntries);
        return responseBuilder
                .build();
    }

    @Override
    public ReceiveMessageResponse receiveMessage(ReceiveMessageRequest receiveMessageRequest) throws OverLimitException, AwsServiceException, SdkClientException, SqsException {
        SimulatedSqsQueue queue = queueByUrl.get(receiveMessageRequest.queueUrl());
        if (queue == null) {
            throw new IllegalArgumentException("Queue does not exists: " + receiveMessageRequest.queueUrl());
        }
        ReceiveMessageResponse.Builder responseBuilder = ReceiveMessageResponse.builder();
        Map.Entry<Long, Message> firstEntry;
        firstEntry = queue.primary.pollFirstEntry();
        Integer waitTimeSeconds = receiveMessageRequest.waitTimeSeconds();
        if (waitTimeSeconds != null) {
            final Instant expiry = Instant.now();
            while (Instant.now().isBefore(expiry) && firstEntry == null) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                firstEntry = queue.primary.pollFirstEntry();
            }
        }
        if (firstEntry != null) {
            Message message = firstEntry.getValue();
            queue.delivered.put(firstEntry.getKey(), message);
            responseBuilder.messages(message);
        } else {
            responseBuilder.messages(Collections.emptyList());
        }
        responseBuilder.sdkHttpResponse(SdkHttpResponse.builder()
                .statusCode(200)
                .build());
        return responseBuilder.build();
    }

    @Override
    public DeleteMessageResponse deleteMessage(DeleteMessageRequest deleteMessageRequest) throws InvalidIdFormatException, ReceiptHandleIsInvalidException, AwsServiceException, SdkClientException, SqsException {
        SimulatedSqsQueue queue = queueByUrl.get(deleteMessageRequest.queueUrl());
        Message removedMessage = queue.delivered.remove(Long.valueOf(deleteMessageRequest.receiptHandle()));
        if (removedMessage == null) {
            throw new IllegalArgumentException("Non-existed message: " + deleteMessageRequest.receiptHandle());
        }
        DeleteMessageResponse.Builder responseBuilder = DeleteMessageResponse.builder();
        responseBuilder.sdkHttpResponse(SdkHttpResponse.builder()
                .statusCode(200)
                .build());
        return responseBuilder
                .build();
    }
}
