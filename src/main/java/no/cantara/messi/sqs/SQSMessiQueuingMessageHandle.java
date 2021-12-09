package no.cantara.messi.sqs;

import no.cantara.messi.api.MessiQueuingMessageHandle;
import no.cantara.messi.protos.MessiMessage;

import java.util.function.Consumer;

public class SQSMessiQueuingMessageHandle implements MessiQueuingMessageHandle {

    private final MessiMessage message;
    private final String receiptHandle;
    private final Consumer<String> ackConsumer;

    public SQSMessiQueuingMessageHandle(MessiMessage message, String receiptHandle, Consumer<String> ackConsumer) {
        this.message = message;
        this.receiptHandle = receiptHandle;
        this.ackConsumer = ackConsumer;
    }

    @Override
    public MessiMessage message() {
        return message;
    }

    @Override
    public void ack() {
        ackConsumer.accept(receiptHandle);
    }
}
