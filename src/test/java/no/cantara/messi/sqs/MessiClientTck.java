package no.cantara.messi.sqs;

import com.google.protobuf.ByteString;
import de.huxhorn.sulky.ulid.ULID;
import no.cantara.config.ApplicationProperties;
import no.cantara.config.ProviderLoader;
import no.cantara.messi.api.MessiClient;
import no.cantara.messi.api.MessiClientFactory;
import no.cantara.messi.api.MessiConsumer;
import no.cantara.messi.api.MessiProducer;
import no.cantara.messi.api.MessiULIDUtils;
import no.cantara.messi.protos.MessiMessage;
import no.cantara.messi.protos.MessiOrdering;
import no.cantara.messi.protos.MessiProvider;
import no.cantara.messi.protos.MessiSource;
import no.cantara.messi.protos.MessiUlid;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class MessiClientTck {

    MessiClient client;

    @BeforeMethod
    public void createMessiClient() {
        ApplicationProperties applicationProperties = ApplicationProperties.builder()
                .testDefaults()
                .build();
        client = ProviderLoader.configure(applicationProperties, "sqs-simulator", MessiClientFactory.class);
    }

    @AfterMethod
    public void closeMessiClient() {
        client.close();
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void thatLastMessageIsUnsupported() {
        assertNull(client.lastMessage("the-topic"));
    }

    @Test
    public void thatAllFieldsOfMessageSurvivesStream() throws Exception {
        ULID.Value ulid = new ULID().nextValue();
        try (MessiProducer producer = client.producer("the-topic")) {
            producer.publish(
                    MessiMessage.newBuilder()
                            .setPartitionKey("pk1")
                            .setUlid(MessiUlid.newBuilder().setMsb(ulid.getMostSignificantBits()).setLsb(ulid.getLeastSignificantBits()).build())
                            .setOrdering(MessiOrdering.newBuilder()
                                    .setGroup("og1")
                                    .setSequenceNumber(1)
                                    .build())
                            .setExternalId("a")
                            .putData("payload1", ByteString.copyFromUtf8("p1"))
                            .putData("payload2", ByteString.copyFromUtf8("p2"))
                            .build(),
                    MessiMessage.newBuilder()
                            .setPartitionKey("pk1")
                            .setOrdering(MessiOrdering.newBuilder()
                                    .setGroup("og1")
                                    .setSequenceNumber(2)
                                    .build())
                            .setExternalId("b")
                            .putData("payload1", ByteString.copyFromUtf8("p3"))
                            .putData("payload2", ByteString.copyFromUtf8("p4"))
                            .build(),
                    MessiMessage.newBuilder()
                            .setPartitionKey("pk1")
                            .setOrdering(MessiOrdering.newBuilder()
                                    .setGroup("og1")
                                    .setSequenceNumber(3)
                                    .build())
                            .setExternalId("c")
                            .putData("payload1", ByteString.copyFromUtf8("p5"))
                            .putData("payload2", ByteString.copyFromUtf8("p6"))
                            .setProvider(MessiProvider.newBuilder()
                                    .setPublishedTimestamp(123)
                                    .setShardId("shardId123")
                                    .setSequenceNumber("three")
                                    .build())
                            .setSource(MessiSource.newBuilder()
                                    .setClientSourceId("client-source-id-123")
                                    .build())
                            .putAttributes("key1", "value1")
                            .putAttributes("some-other-key", "some other value")
                            .putAttributes("iamanattribute", "yes I am")
                            .build()
            );
        }


        try (MessiConsumer consumer = client.consumer("the-topic")) {
            {
                MessiMessage message = consumer.receive(1, TimeUnit.SECONDS);
                assertEquals(MessiULIDUtils.toUlid(message.getUlid()), ulid);
                assertEquals(message.getPartitionKey(), "pk1");
                assertEquals(message.getOrdering().getGroup(), "og1");
                assertEquals(message.getOrdering().getSequenceNumber(), 1);
                assertEquals(message.getExternalId(), "a");
                assertEquals(message.getDataCount(), 2);
                assertEquals(message.getDataOrThrow("payload1"), ByteString.copyFromUtf8("p1"));
                assertEquals(message.getDataOrThrow("payload2"), ByteString.copyFromUtf8("p2"));
            }
            {
                MessiMessage message = consumer.receive(1, TimeUnit.SECONDS);
                assertEquals(message.getPartitionKey(), "pk1");
                assertNotNull(message.getUlid());
                assertEquals(message.getOrdering().getGroup(), "og1");
                assertEquals(message.getOrdering().getSequenceNumber(), 2);
                assertEquals(message.getExternalId(), "b");
                assertEquals(message.getDataCount(), 2);
                assertEquals(message.getDataOrThrow("payload1"), ByteString.copyFromUtf8("p3"));
                assertEquals(message.getDataOrThrow("payload2"), ByteString.copyFromUtf8("p4"));
            }
            {
                MessiMessage message = consumer.receive(1, TimeUnit.SECONDS);
                assertEquals(message.getPartitionKey(), "pk1");
                assertNotNull(message.getUlid());
                assertEquals(message.getOrdering().getGroup(), "og1");
                assertEquals(message.getOrdering().getSequenceNumber(), 3);
                assertEquals(message.getExternalId(), "c");
                assertEquals(message.getDataCount(), 2);
                assertEquals(message.getDataOrThrow("payload1"), ByteString.copyFromUtf8("p5"));
                assertEquals(message.getDataOrThrow("payload2"), ByteString.copyFromUtf8("p6"));
                assertTrue(message.hasProvider());
                assertNotNull(message.getProvider());
                assertNotEquals(message.getProvider().getPublishedTimestamp(), 123);
                assertFalse(message.getProvider().hasShardId());
                assertTrue(message.getProvider().hasSequenceNumber());
                assertEquals(message.getProvider().getSequenceNumber(), "3"); // SQS will not touch provider
                assertEquals(message.getSource().getClientSourceId(), "client-source-id-123");
                assertEquals(message.getAttributesCount(), 3);
                assertEquals(message.getAttributesOrThrow("key1"), "value1");
                assertEquals(message.getAttributesOrThrow("some-other-key"), "some other value");
                assertEquals(message.getAttributesOrThrow("iamanattribute"), "yes I am");
            }
        }
    }

    @Test
    public void thatSingleMessageCanBeProducedAndConsumerSynchronously() throws InterruptedException {
        try (MessiConsumer consumer = client.consumer("the-topic")) {

            try (MessiProducer producer = client.producer("the-topic")) {
                producer.publish(MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("a").putData("payload1", ByteString.copyFrom(new byte[5])).putData("payload2", ByteString.copyFrom(new byte[5])).build());
            }

            MessiMessage message = consumer.receive(5, TimeUnit.SECONDS);
            assertEquals(message.getExternalId(), "a");
            assertEquals(message.getDataCount(), 2);
        }
    }

    @Test
    public void thatSingleMessageCanBeProducedAndConsumerAsynchronously() {
        try (MessiConsumer consumer = client.consumer("the-topic")) {

            CompletableFuture<? extends MessiMessage> future = consumer.receiveAsync();

            try (MessiProducer producer = client.producer("the-topic")) {
                producer.publish(MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("a").putData("payload1", ByteString.copyFrom(new byte[5])).putData("payload2", ByteString.copyFrom(new byte[5])).build());
            }

            MessiMessage message = future.join();
            assertEquals(message.getExternalId(), "a");
            assertEquals(message.getDataCount(), 2);
        }
    }

    @Test
    public void thatMultipleMessagesCanBeProducedAndConsumerSynchronously() throws InterruptedException {
        try (MessiConsumer consumer = client.consumer("the-topic")) {

            try (MessiProducer producer = client.producer("the-topic")) {
                producer.publish(
                        MessiMessage.newBuilder().setPartitionKey("pk1").setUlid(MessiULIDUtils.toMessiUlid(new ULID().nextValue())).setExternalId("a").putData("payload1", ByteString.copyFrom(new byte[5])).putData("payload2", ByteString.copyFrom(new byte[5])).build(),
                        MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("b").putData("payload1", ByteString.copyFrom(new byte[3])).putData("payload2", ByteString.copyFrom(new byte[3])).build(),
                        MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("c").putData("payload1", ByteString.copyFrom(new byte[7])).putData("payload2", ByteString.copyFrom(new byte[7])).build()
                );
            }

            MessiMessage message1 = consumer.receive(5, TimeUnit.SECONDS);
            MessiMessage message2 = consumer.receive(1, TimeUnit.SECONDS);
            MessiMessage message3 = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message1.getExternalId(), "a");
            assertEquals(message2.getExternalId(), "b");
            assertEquals(message3.getExternalId(), "c");
        }
    }

    @Test
    public void thatMultipleMessagesCanBeProducedAndConsumerAsynchronously() {
        try (MessiConsumer consumer = client.consumer("the-topic")) {

            CompletableFuture<List<MessiMessage>> future = receiveAsyncUntilNoMoreMessages(consumer, new ArrayList<>());

            try (MessiProducer producer = client.producer("the-topic")) {
                producer.publish(
                        MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("a").putData("payload1", ByteString.copyFrom(new byte[5])).putData("payload2", ByteString.copyFrom(new byte[5])).build(),
                        MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("b").putData("payload1", ByteString.copyFrom(new byte[3])).putData("payload2", ByteString.copyFrom(new byte[3])).build(),
                        MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("c").putData("payload1", ByteString.copyFrom(new byte[7])).putData("payload2", ByteString.copyFrom(new byte[7])).build()
                );
            }

            List<MessiMessage> messages = future.join();

            assertEquals(messages.get(0).getExternalId(), "a");
            assertEquals(messages.get(1).getExternalId(), "b");
            assertEquals(messages.get(2).getExternalId(), "c");
        }
    }

    private CompletableFuture<List<MessiMessage>> receiveAsyncUntilNoMoreMessages(MessiConsumer consumer, List<MessiMessage> messages) {
        CompletableFuture<List<MessiMessage>> result = new CompletableFuture<>();
        new Thread(() -> {
            try {
                MessiMessage message = consumer.receive(5, TimeUnit.SECONDS);
                while (message != null) {
                    messages.add(message);
                    message = consumer.receive(1, TimeUnit.SECONDS);
                }
                result.complete(messages);
            } catch (InterruptedException e) {
                result.completeExceptionally(e);
            }
        }).start();
        return result;
    }

    @Test
    public void thatMessagesCanBeConsumedByMultipleConsumers() {
        try (MessiConsumer consumer1 = client.consumer("the-topic");
             MessiConsumer consumer2 = client.consumer("the-topic")) {

            CompletableFuture<List<MessiMessage>> future1 = receiveAsyncUntilNoMoreMessages(consumer1, new ArrayList<>());
            CompletableFuture<List<MessiMessage>> future2 = receiveAsyncUntilNoMoreMessages(consumer2, new ArrayList<>());

            try (MessiProducer producer = client.producer("the-topic")) {
                producer.publish(
                        MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("a").putData("payload1", ByteString.copyFrom(new byte[5])).putData("payload2", ByteString.copyFrom(new byte[5])).build(),
                        MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("b").putData("payload1", ByteString.copyFrom(new byte[3])).putData("payload2", ByteString.copyFrom(new byte[3])).build(),
                        MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("c").putData("payload1", ByteString.copyFrom(new byte[7])).putData("payload2", ByteString.copyFrom(new byte[7])).build(),
                        MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("d").putData("payload1", ByteString.copyFrom(new byte[7])).putData("payload2", ByteString.copyFrom(new byte[7])).build(),
                        MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("e").putData("payload1", ByteString.copyFrom(new byte[7])).putData("payload2", ByteString.copyFrom(new byte[7])).build(),
                        MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("f").putData("payload1", ByteString.copyFrom(new byte[7])).putData("payload2", ByteString.copyFrom(new byte[7])).build(),
                        MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("g").putData("payload1", ByteString.copyFrom(new byte[7])).putData("payload2", ByteString.copyFrom(new byte[7])).build()
                );
            }

            List<MessiMessage> messages1 = future1.join();
            List<MessiMessage> messages2 = future2.join();

            List<MessiMessage> union = new ArrayList<>();
            union.addAll(messages1);
            union.addAll(messages2);

            Set<String> externalIds = new LinkedHashSet<>();
            for (MessiMessage message : union) {
                externalIds.add(message.getExternalId());
            }

            assertEquals(externalIds.size(), 7);
            assertTrue(externalIds.contains("a"));
            assertTrue(externalIds.contains("b"));
            assertTrue(externalIds.contains("c"));
            assertTrue(externalIds.contains("d"));
            assertTrue(externalIds.contains("e"));
            assertTrue(externalIds.contains("f"));
            assertTrue(externalIds.contains("g"));
        }
    }

    @Test
    public void thatConsumerCanReadFromBeginning() throws Exception {
        try (MessiProducer producer = client.producer("the-topic")) {
            producer.publish(
                    MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("a").putData("payload1", ByteString.copyFrom(new byte[5])).putData("payload2", ByteString.copyFrom(new byte[5])).build(),
                    MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("b").putData("payload1", ByteString.copyFrom(new byte[3])).putData("payload2", ByteString.copyFrom(new byte[3])).build(),
                    MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("c").putData("payload1", ByteString.copyFrom(new byte[7])).putData("payload2", ByteString.copyFrom(new byte[7])).build(),
                    MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("d").putData("payload1", ByteString.copyFrom(new byte[8])).putData("payload2", ByteString.copyFrom(new byte[8])).build()
            );
        }
        try (MessiConsumer consumer = client.consumer("the-topic")) {
            MessiMessage message = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message.getExternalId(), "a");
        }
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void thatMetadataIsUnsupported() {
        client.metadata("the-topic");
    }
}
