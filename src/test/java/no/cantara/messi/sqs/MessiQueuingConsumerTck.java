package no.cantara.messi.sqs;

import com.google.protobuf.ByteString;
import de.huxhorn.sulky.ulid.ULID;
import no.cantara.config.ApplicationProperties;
import no.cantara.config.ProviderLoader;
import no.cantara.messi.api.MessiClient;
import no.cantara.messi.api.MessiClientFactory;
import no.cantara.messi.api.MessiProducer;
import no.cantara.messi.api.MessiQueuingAsyncMessageHandle;
import no.cantara.messi.api.MessiQueuingConsumer;
import no.cantara.messi.api.MessiQueuingMessageHandle;
import no.cantara.messi.api.MessiShard;
import no.cantara.messi.api.MessiTimestampUtils;
import no.cantara.messi.api.MessiTopic;
import no.cantara.messi.api.MessiULIDUtils;
import no.cantara.messi.protos.MessiMessage;
import no.cantara.messi.protos.MessiOrdering;
import no.cantara.messi.protos.MessiProvider;
import no.cantara.messi.protos.MessiSource;
import no.cantara.messi.protos.MessiUlid;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class MessiQueuingConsumerTck {

    MessiClient client;
    MessiTopic topic;
    MessiShard shard;

    @BeforeMethod
    public void createMessiClient() {
        ApplicationProperties applicationProperties = ApplicationProperties.builder()
                .testDefaults()
                .build();
        client = ProviderLoader.configure(applicationProperties, "sqs-simulator", MessiClientFactory.class);
        topic = client.topicOf("the-topic");
        shard = topic.shardOf(topic.firstShard());
        assertTrue(shard.supportsQueuing());
    }

    @AfterMethod
    public void closeMessiClient() {
        client.close();
    }

    @Test
    public void thatAllFieldsOfMessageSurvivesStream() throws Exception {
        ULID.Value ulid = new ULID().nextValue();
        try (MessiProducer producer = topic.producer()) {
            producer.publish(
                    MessiMessage.newBuilder()
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
                            .setOrdering(MessiOrdering.newBuilder()
                                    .setGroup("og1")
                                    .setSequenceNumber(2)
                                    .build())
                            .setExternalId("b")
                            .putData("payload1", ByteString.copyFromUtf8("p3"))
                            .putData("payload2", ByteString.copyFromUtf8("p4"))
                            .build(),
                    MessiMessage.newBuilder()
                            .setTimestamp(MessiTimestampUtils.toTimestamp(Instant.parse("2021-12-09T11:59:18.052Z")))
                            .setOrdering(MessiOrdering.newBuilder()
                                    .setGroup("og1")
                                    .setSequenceNumber(3)
                                    .build())
                            .setExternalId("c")
                            .putData("payload1", ByteString.copyFromUtf8("p5"))
                            .putData("payload2", ByteString.copyFromUtf8("p6"))
                            .setFirstProvider(MessiProvider.newBuilder()
                                    .setTechnology("JUNIT")
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

        try (MessiQueuingConsumer consumer = shard.queuingConsumer()) {
            {
                MessiQueuingMessageHandle handle = consumer.receive(1, TimeUnit.SECONDS);
                handle.ack();
                MessiMessage message = handle.message();
                assertEquals(MessiULIDUtils.toUlid(message.getUlid()), ulid);
                assertEquals(message.getOrdering().getGroup(), "og1");
                assertEquals(message.getOrdering().getSequenceNumber(), 1);
                assertEquals(message.getExternalId(), "a");
                assertEquals(message.getDataCount(), 2);
                assertEquals(message.getDataOrThrow("payload1"), ByteString.copyFromUtf8("p1"));
                assertEquals(message.getDataOrThrow("payload2"), ByteString.copyFromUtf8("p2"));
            }
            {
                MessiQueuingMessageHandle handle = consumer.receive(1, TimeUnit.SECONDS);
                handle.ack();
                MessiMessage message = handle.message();
                assertNotNull(message.getUlid());
                assertEquals(message.getOrdering().getGroup(), "og1");
                assertEquals(message.getOrdering().getSequenceNumber(), 2);
                assertEquals(message.getExternalId(), "b");
                assertEquals(message.getDataCount(), 2);
                assertEquals(message.getDataOrThrow("payload1"), ByteString.copyFromUtf8("p3"));
                assertEquals(message.getDataOrThrow("payload2"), ByteString.copyFromUtf8("p4"));
            }
            {
                MessiQueuingMessageHandle handle = consumer.receive(1, TimeUnit.SECONDS);
                handle.ack();
                MessiMessage message = handle.message();
                assertNotNull(message.getUlid());
                assertEquals(message.getTimestamp(), MessiTimestampUtils.toTimestamp(Instant.parse("2021-12-09T11:59:18.052Z")));
                assertEquals(message.getOrdering().getGroup(), "og1");
                assertEquals(message.getOrdering().getSequenceNumber(), 3);
                assertEquals(message.getExternalId(), "c");
                assertEquals(message.getDataCount(), 2);
                assertEquals(message.getDataOrThrow("payload1"), ByteString.copyFromUtf8("p5"));
                assertEquals(message.getDataOrThrow("payload2"), ByteString.copyFromUtf8("p6"));
                assertEquals(message.getFirstProvider().getPublishedTimestamp(), 123);
                assertEquals(message.getFirstProvider().getShardId(), "shardId123");
                assertEquals(message.getFirstProvider().getSequenceNumber(), "three");
                assertEquals(message.getFirstProvider().getTechnology(), "JUNIT");
                assertEquals(message.getProvider().getTechnology(), "SQS");
                assertNotEquals(message.getProvider().getPublishedTimestamp(), 0);
                assertNotEquals(message.getProvider().getPublishedTimestamp(), 123);
                assertEquals(message.getProvider().getSequenceNumber(), "3");
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
        try (MessiQueuingConsumer consumer = shard.queuingConsumer()) {

            try (MessiProducer producer = topic.producer()) {
                producer.publish(MessiMessage.newBuilder().setExternalId("a").putData("payload1", ByteString.copyFrom(new byte[5])).putData("payload2", ByteString.copyFrom(new byte[5])).build());
            }

            MessiQueuingMessageHandle handle = consumer.receive(5, TimeUnit.SECONDS);
            handle.ack();
            MessiMessage message = handle.message();
            assertEquals(message.getExternalId(), "a");
            assertEquals(message.getDataCount(), 2);
        }
    }

    @Test
    public void thatSingleMessageCanBeProducedAndConsumerAsynchronously() {
        try (MessiQueuingConsumer consumer = shard.queuingConsumer()) {

            CompletableFuture<? extends MessiQueuingAsyncMessageHandle> future = consumer.receiveAsync();

            try (MessiProducer producer = topic.producer()) {
                producer.publish(MessiMessage.newBuilder().setExternalId("a").putData("payload1", ByteString.copyFrom(new byte[5])).putData("payload2", ByteString.copyFrom(new byte[5])).build());
            }

            MessiQueuingAsyncMessageHandle asyncHandle = future.join();
            asyncHandle.ack().join();
            MessiMessage message = asyncHandle.message();
            assertEquals(message.getExternalId(), "a");
            assertEquals(message.getDataCount(), 2);
        }
    }

    @Test
    public void thatMultipleMessagesCanBeProducedAndConsumerSynchronously() throws InterruptedException {
        try (MessiQueuingConsumer consumer = shard.queuingConsumer()) {

            try (MessiProducer producer = topic.producer()) {
                producer.publish(
                        MessiMessage.newBuilder().setUlid(MessiULIDUtils.toMessiUlid(new ULID().nextValue())).setExternalId("a").putData("payload1", ByteString.copyFrom(new byte[5])).putData("payload2", ByteString.copyFrom(new byte[5])).build(),
                        MessiMessage.newBuilder().setExternalId("b").putData("payload1", ByteString.copyFrom(new byte[3])).putData("payload2", ByteString.copyFrom(new byte[3])).build(),
                        MessiMessage.newBuilder().setExternalId("c").putData("payload1", ByteString.copyFrom(new byte[7])).putData("payload2", ByteString.copyFrom(new byte[7])).build()
                );
            }

            MessiQueuingMessageHandle handle = consumer.receive(5, TimeUnit.SECONDS);
            handle.ack();
            MessiMessage message1 = handle.message();
            MessiQueuingMessageHandle handle2 = consumer.receive(1, TimeUnit.SECONDS);
            handle2.ack();
            MessiMessage message2 = handle2.message();
            MessiQueuingMessageHandle handle3 = consumer.receive(1, TimeUnit.SECONDS);
            handle3.ack();
            MessiMessage message3 = handle3.message();
            assertEquals(message1.getExternalId(), "a");
            assertEquals(message2.getExternalId(), "b");
            assertEquals(message3.getExternalId(), "c");
        }
    }

    @Test
    public void thatMultipleMessagesCanBeProducedAndConsumerAsynchronously() {
        try (MessiQueuingConsumer consumer = shard.queuingConsumer()) {

            CompletableFuture<List<MessiMessage>> future = receiveAsyncAddMessageAndRepeatRecursive(consumer, "c", new ArrayList<>());

            try (MessiProducer producer = topic.producer()) {
                producer.publish(
                        MessiMessage.newBuilder().setExternalId("a").putData("payload1", ByteString.copyFrom(new byte[5])).putData("payload2", ByteString.copyFrom(new byte[5])).build(),
                        MessiMessage.newBuilder().setExternalId("b").putData("payload1", ByteString.copyFrom(new byte[3])).putData("payload2", ByteString.copyFrom(new byte[3])).build(),
                        MessiMessage.newBuilder().setExternalId("c").putData("payload1", ByteString.copyFrom(new byte[7])).putData("payload2", ByteString.copyFrom(new byte[7])).build()
                );
            }

            List<MessiMessage> messages = future.join();

            assertEquals(messages.get(0).getExternalId(), "a");
            assertEquals(messages.get(1).getExternalId(), "b");
            assertEquals(messages.get(2).getExternalId(), "c");
        }
    }

    private CompletableFuture<List<MessiMessage>> receiveAsyncAddMessageAndRepeatRecursive(MessiQueuingConsumer consumer, String endExternalId, List<MessiMessage> messages) {
        return consumer.receiveAsync().thenCompose(asyncHandle -> {
            MessiMessage message = asyncHandle.message();
            asyncHandle.ack();
            messages.add(message);
            if (endExternalId.equals(message.getExternalId())) {
                return CompletableFuture.completedFuture(messages);
            }
            return receiveAsyncAddMessageAndRepeatRecursive(consumer, endExternalId, messages);
        });
    }

    @Test
    public void thatMessagesCanBeConsumedByMultipleConsumers() throws InterruptedException {
        try (MessiQueuingConsumer consumer1 = shard.queuingConsumer();
             MessiQueuingConsumer consumer2 = shard.queuingConsumer()) {

            CountDownLatch cdl = new CountDownLatch(8);

            List<MessiMessage> messages = Collections.synchronizedList(new ArrayList<>());

            new Thread(() -> {
                try {
                    MessiQueuingMessageHandle handle;
                    do {
                        handle = consumer1.receive(100, TimeUnit.MILLISECONDS);
                        if (handle != null) {
                            Thread.sleep(10); // force race-condition if topic has not reserved message to consumer
                            handle.ack();
                            MessiMessage message = handle.message();
                            if ("DONE".equals(message.getExternalId())) {
                                try (MessiProducer producer = topic.producer()) {
                                    producer.publish(MessiMessage.newBuilder().setExternalId("DONE").build());
                                }
                                return;
                            }
                            messages.add(message);
                            cdl.countDown();
                        }
                    } while (true);
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }).start();

            new Thread(() -> {
                try {
                    MessiQueuingMessageHandle handle;
                    do {
                        handle = consumer2.receive(100, TimeUnit.MILLISECONDS);
                        if (handle != null) {
                            Thread.sleep(10); // force race-condition if topic has not reserved message to consumer
                            handle.ack();
                            MessiMessage message = handle.message();
                            if ("DONE".equals(message.getExternalId())) {
                                try (MessiProducer producer = topic.producer()) {
                                    producer.publish(MessiMessage.newBuilder().setExternalId("DONE").build());
                                }
                                return;
                            }
                            messages.add(message);
                            cdl.countDown();
                        }
                    } while (true);
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }).start();

            try (MessiProducer producer = topic.producer()) {
                producer.publish(
                        MessiMessage.newBuilder().setExternalId("a").putData("payload1", ByteString.copyFrom(new byte[5])).putData("payload2", ByteString.copyFrom(new byte[5])).build(),
                        MessiMessage.newBuilder().setExternalId("b").putData("payload1", ByteString.copyFrom(new byte[3])).putData("payload2", ByteString.copyFrom(new byte[3])).build(),
                        MessiMessage.newBuilder().setExternalId("c").putData("payload1", ByteString.copyFrom(new byte[7])).putData("payload2", ByteString.copyFrom(new byte[7])).build(),
                        MessiMessage.newBuilder().setExternalId("d").putData("payload1", ByteString.copyFrom(new byte[7])).putData("payload2", ByteString.copyFrom(new byte[7])).build(),
                        MessiMessage.newBuilder().setExternalId("e").putData("payload1", ByteString.copyFrom(new byte[7])).putData("payload2", ByteString.copyFrom(new byte[7])).build(),
                        MessiMessage.newBuilder().setExternalId("f").putData("payload1", ByteString.copyFrom(new byte[7])).putData("payload2", ByteString.copyFrom(new byte[7])).build(),
                        MessiMessage.newBuilder().setExternalId("g").putData("payload1", ByteString.copyFrom(new byte[7])).putData("payload2", ByteString.copyFrom(new byte[7])).build(),
                        MessiMessage.newBuilder().setExternalId("h").putData("payload1", ByteString.copyFrom(new byte[7])).putData("payload2", ByteString.copyFrom(new byte[7])).build(),
                        MessiMessage.newBuilder().setExternalId("DONE").build()
                );
            }

            assertTrue(cdl.await(5, TimeUnit.SECONDS));
            Thread.sleep(300); // allow more than 8 messages to capture possible wrong topic queuing behavior, typically messages are not reserved exclusively to consumer
            assertEquals(messages.size(), 8);
        }
    }

    @Test
    public void thatConsumerCanReadFromBeginning() throws Exception {
        try (MessiProducer producer = topic.producer()) {
            producer.publish(
                    MessiMessage.newBuilder().setExternalId("a").putData("payload1", ByteString.copyFrom(new byte[5])).putData("payload2", ByteString.copyFrom(new byte[5])).build(),
                    MessiMessage.newBuilder().setExternalId("b").putData("payload1", ByteString.copyFrom(new byte[3])).putData("payload2", ByteString.copyFrom(new byte[3])).build(),
                    MessiMessage.newBuilder().setExternalId("c").putData("payload1", ByteString.copyFrom(new byte[7])).putData("payload2", ByteString.copyFrom(new byte[7])).build(),
                    MessiMessage.newBuilder().setExternalId("d").putData("payload1", ByteString.copyFrom(new byte[8])).putData("payload2", ByteString.copyFrom(new byte[8])).build()
            );
        }
        try (MessiQueuingConsumer consumer = shard.queuingConsumer()) {
            MessiQueuingMessageHandle handle = consumer.receive(1, TimeUnit.SECONDS);
            handle.ack();
            MessiMessage message = handle.message();
            assertEquals(message.getExternalId(), "a");
        }
    }
}
