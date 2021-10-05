package no.cantara.messi.sqs;

import no.cantara.config.ApplicationProperties;
import no.cantara.messi.api.MessiClientFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

public class SQSMessiClientFactory implements MessiClientFactory {

    @Override
    public Class<?> providerClass() {
        return SQSMessiClient.class;
    }

    @Override
    public String alias() {
        return "sqs";
    }

    @Override
    public SQSMessiClient create(ApplicationProperties configuration) {

        String region = configuration.get("region");
        if (region == null) {
            throw new IllegalArgumentException("Missing configuration property: region");
        }
        String accessKeyId = configuration.get("credentials.keyid");
        if (accessKeyId == null) {
            throw new IllegalArgumentException("Missing configuration property: credentials.keyid");
        }
        String secretAccessKey = configuration.get("credentials.secret");
        if (secretAccessKey == null) {
            throw new IllegalArgumentException("Missing configuration property: credentials.secret");
        }
        String queueNamePrefix = configuration.get("queue-name.prefix");
        if (queueNamePrefix == null) {
            throw new IllegalArgumentException("Missing configuration property: queue-name.prefix");
        }

        AwsBasicCredentials awsCreds = AwsBasicCredentials.create(accessKeyId, secretAccessKey);
        SqsClient sqsClient = SqsClient.builder()
                // apiCallAttemptTimeout should be more than long-polling max time which is 20 seconds
                .overrideConfiguration(config -> config.apiCallAttemptTimeout(Duration.of(30, ChronoUnit.SECONDS)))
                .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                .region(Region.of(region))
                .build();

        return new SQSMessiClient(sqsClient, queueNamePrefix);
    }
}
