package no.cantara.messi.sqs.simulator;

import no.cantara.config.ApplicationProperties;
import no.cantara.messi.api.MessiClientFactory;
import no.cantara.messi.sqs.SQSMessiClient;
import software.amazon.awssdk.services.sqs.SqsClient;

public class SQSSimulatorMessiClientFactory implements MessiClientFactory {

    @Override
    public Class<?> providerClass() {
        return SQSMessiClient.class;
    }

    @Override
    public String alias() {
        return "sqs-simulator";
    }

    @Override
    public SQSMessiClient create(ApplicationProperties configuration) {
        SqsClient sqsClient = new SqsClientSimulator();
        return new SQSMessiClient(sqsClient, "messi-sqs-simulator", true);
    }
}
