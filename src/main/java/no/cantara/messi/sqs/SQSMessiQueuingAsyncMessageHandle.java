package no.cantara.messi.sqs;

import no.cantara.messi.api.MessiQueuingAsyncMessageHandle;
import no.cantara.messi.protos.MessiMessage;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public class SQSMessiQueuingAsyncMessageHandle implements MessiQueuingAsyncMessageHandle {

    private final MessiMessage message;
    private final String receiptHandle;
    private final Consumer<String> ackConsumer;

    public SQSMessiQueuingAsyncMessageHandle(MessiMessage message, String receiptHandle, Consumer<String> ackConsumer) {
        this.message = message;
        this.receiptHandle = receiptHandle;
        this.ackConsumer = ackConsumer;
    }

    @Override
    public MessiMessage message() {
        return message;
    }

    @Override
    public CompletableFuture<Void> ack() {
        return CompletableFuture.supplyAsync(() -> {
            ackConsumer.accept(receiptHandle);
            return null;
        });
    }
}
