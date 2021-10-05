package no.cantara.messi.sqs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueResponse;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.ListQueuesRequest;
import software.amazon.awssdk.services.sqs.model.ListQueuesResponse;

import java.util.ArrayList;
import java.util.List;

public class SQSUtils {

    private static final Logger log = LoggerFactory.getLogger(SQSUtils.class);

    /**
     * @param sqsClient
     * @param queueName
     * @return the queueUrl
     */
    public static String createQueue(SqsClient sqsClient, String queueName) {
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueName)
                .build();
        CreateQueueResponse createQueueResponse = sqsClient.createQueue(createQueueRequest);
        SdkHttpResponse sdkHttpResponse = createQueueResponse.sdkHttpResponse();
        if (!sdkHttpResponse.isSuccessful()) {
            throw new RuntimeException(String.format("Unable to create SQS queue. statusCode=%s, statusText=%s", sdkHttpResponse.statusCode(), sdkHttpResponse.statusText()));
        }
        return createQueueResponse.queueUrl();
    }

    /**
     * List all queues available to sqs-client.
     *
     * @param sqsClient
     * @param prefix
     * @return a list of queue-urls
     */
    public static List<String> listQueues(SqsClient sqsClient, String prefix) {
        List<String> queueUrls = new ArrayList<>(1000);
        String nextToken = null;
        do {
            ListQueuesRequest.Builder requestBuilder = ListQueuesRequest.builder();
            if (nextToken != null) {
                requestBuilder.nextToken(nextToken);
            }
            ListQueuesRequest listQueuesRequest = requestBuilder
                    .maxResults(1000)
                    .queueNamePrefix(prefix)
                    .build();

            ListQueuesResponse listQueuesResponse = sqsClient.listQueues(listQueuesRequest);

            SdkHttpResponse sdkHttpResponse = listQueuesResponse.sdkHttpResponse();
            if (!sdkHttpResponse.isSuccessful()) {
                throw new RuntimeException(String.format("Unable to list SQS queues. statusCode=%s, statusText=%s", sdkHttpResponse.statusCode(), sdkHttpResponse.statusText()));
            }

            queueUrls.addAll(listQueuesResponse.queueUrls());

            nextToken = listQueuesResponse.nextToken();

        } while (nextToken != null);

        return queueUrls;
    }

    public static String getQueueUrl(SqsClient sqsClient, String queueName) {
        GetQueueUrlRequest request = GetQueueUrlRequest.builder().queueName(queueName).build();
        GetQueueUrlResponse response = sqsClient.getQueueUrl(request);
        SdkHttpResponse sdkHttpResponse = response.sdkHttpResponse();
        if (!sdkHttpResponse.isSuccessful()) {
            throw new RuntimeException(String.format("Unable to get queue-url of SQS queue. statusCode=%s, statusText=%s", sdkHttpResponse.statusCode(), sdkHttpResponse.statusText()));
        }
        String queueUrl = response.queueUrl();
        return queueUrl;
    }

    public static void deleteSQSQueue(SqsClient sqsClient, String queueName) {
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(queueName)
                .build();

        String queueUrl = sqsClient.getQueueUrl(getQueueRequest).queueUrl();

        DeleteQueueRequest deleteQueueRequest = DeleteQueueRequest.builder()
                .queueUrl(queueUrl)
                .build();

        DeleteQueueResponse response = sqsClient.deleteQueue(deleteQueueRequest);

        SdkHttpResponse sdkHttpResponse = response.sdkHttpResponse();
        if (!sdkHttpResponse.isSuccessful()) {
            throw new RuntimeException(String.format("Unable to delete SQS queue. statusCode=%s, statusText=%s", sdkHttpResponse.statusCode(), sdkHttpResponse.statusText()));
        }
    }
}
