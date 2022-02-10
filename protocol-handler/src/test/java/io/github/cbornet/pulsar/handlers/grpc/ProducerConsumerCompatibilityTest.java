/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.cbornet.pulsar.handlers.grpc;

import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandAck.AckType;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandGetLastMessageIdResponse;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandGetOrCreateSchema;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandGetOrCreateSchemaResponse;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandGetSchema;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandGetSchemaResponse;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandLookupTopic;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandLookupTopicResponse;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandLookupTopicResponse.LookupType;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandMessage;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandPartitionedTopicMetadata;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandPartitionedTopicMetadataResponse;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandProducer;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandSend;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandSubscribe;
import io.github.cbornet.pulsar.handlers.grpc.api.CompressionType;
import io.github.cbornet.pulsar.handlers.grpc.api.ConsumeInput;
import io.github.cbornet.pulsar.handlers.grpc.api.ConsumeOutput;
import io.github.cbornet.pulsar.handlers.grpc.api.MessageIdData;
import io.github.cbornet.pulsar.handlers.grpc.api.MessageMetadata;
import io.github.cbornet.pulsar.handlers.grpc.api.Messages;
import io.github.cbornet.pulsar.handlers.grpc.api.MetadataAndPayload;
import io.github.cbornet.pulsar.handlers.grpc.api.PayloadType;
import io.github.cbornet.pulsar.handlers.grpc.api.PulsarGrpc;
import io.github.cbornet.pulsar.handlers.grpc.api.Schema;
import io.github.cbornet.pulsar.handlers.grpc.api.SendResult;
import io.github.cbornet.pulsar.handlers.grpc.api.SingleMessage;
import io.grpc.ManagedChannel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.bookkeeper.util.PortManager;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.EncryptionKeyInfo;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.schema.LongSchemaVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.apache.pulsar.common.protocol.Commands.parseMessageMetadata;
import static org.mockito.Mockito.doReturn;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Compatibility tests between Pulsar binary client and gRPC client.
 */
public class ProducerConsumerCompatibilityTest extends ProducerConsumerBase {

    private static final Logger log = LoggerFactory.getLogger(ProducerConsumerCompatibilityTest.class);

    private GrpcService grpcService;
    private PulsarGrpc.PulsarStub stub;
    private PulsarGrpc.PulsarBlockingStub blockingStub;
    private ManagedChannel channel;

    int port;

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        port = PortManager.nextFreePort();
        super.internalSetup();
        super.producerBaseSetup();

        grpcService = new GrpcService();

        conf.getProperties().setProperty("grpcServicePort", String.valueOf(port));
        grpcService.initialize(conf);
        grpcService.start(pulsar.getBrokerService());

        NettyChannelBuilder channelBuilder = NettyChannelBuilder
                .forAddress("localhost", grpcService.getListenPort().orElse(-1))
                .usePlaintext()
                .negotiationType(NegotiationType.PLAINTEXT);
        channel = channelBuilder.build();
        stub = PulsarGrpc.newStub(channel);
        blockingStub = PulsarGrpc.newBlockingStub(channel);
    }

    protected void beforePulsarStartMocks(PulsarService pulsar) throws Exception {
        Map<String, String> protocolDataToAdvertise = new HashMap<>();
        protocolDataToAdvertise.put("grpc", "grpcServiceHost=localhost;grpcServicePort=" + port);
        doReturn(protocolDataToAdvertise).when(pulsar).getProtocolDataToAdvertise();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        channel.shutdown();
        channel.awaitTermination(30, TimeUnit.SECONDS);
        grpcService.close();
        super.internalCleanup();
    }

    @Test
    public void testPulsarProducerAndGrpcBinaryConsumer() throws Exception {
        log.info("-- Starting {} test --", methodName);

        // Lookup
        PulsarGrpc.PulsarBlockingStub blockingStub = PulsarGrpc.newBlockingStub(channel);
        blockingStub.lookupTopic(Commands.newLookup("persistent://my-property/my-ns/my-topic1", false));

        // Subscribe
        CommandSubscribe subscribe = Commands.newSubscribe("persistent://my-property/my-ns/my-topic1",
                "my-subscriber-name", CommandSubscribe.SubType.Exclusive, 0,
                "test", 0, PayloadType.BINARY);
        PulsarGrpc.PulsarStub consumerStub = Commands.attachConsumerParams(stub, subscribe);

        TestStreamObserver<ConsumeOutput> consumeOutput = TestStreamObserver.create();
        StreamObserver<ConsumeInput> consumeInput = consumerStub.consume(consumeOutput);

        assertTrue(consumeOutput.takeOneMessage().hasSubscribeSuccess());

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer()
                .enableBatching(false)
                .topic("persistent://my-property/my-ns/my-topic1");

        Producer<byte[]> producer = producerBuilder.create();
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        CommandMessage message = null;
        Set<String> messageSet = Sets.newHashSet();
        for (int i = 0; i < 10; i++) {
            message = consumeOutput.takeOneMessage().getMessage();
            ByteBuf headersAndPayload = Unpooled.wrappedBuffer(message.getBinaryMetadataAndPayload().toByteArray());
            parseMessageMetadata(headersAndPayload);
            ByteBuf payload = Unpooled.copiedBuffer(headersAndPayload);
            String receivedMessage = new String(payload.array());
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }
        // Acknowledge the consumption of all messages at once
        consumeInput.onNext(Commands.newAck(message.getMessageId(), AckType.Cumulative));
        Thread.sleep(100);
        consumeInput.onCompleted();
        consumeOutput.waitForCompletion();
        log.info("-- Exiting {} test --", methodName);
    }

    @Test
    public void testPulsarProducerAndGrpcMetadataAndPayloadConsumer() throws Exception {
        log.info("-- Starting {} test --", methodName);

        // Lookup
        PulsarGrpc.PulsarBlockingStub blockingStub = PulsarGrpc.newBlockingStub(channel);
        blockingStub.lookupTopic(Commands.newLookup("persistent://my-property/my-ns/my-topic1", false));

        // Subscribe
        CommandSubscribe subscribe = Commands.newSubscribe("persistent://my-property/my-ns/my-topic1",
                "my-subscriber-name", CommandSubscribe.SubType.Exclusive, 0,
                "test", 0, PayloadType.METADATA_AND_PAYLOAD);
        PulsarGrpc.PulsarStub consumerStub = Commands.attachConsumerParams(stub, subscribe);

        TestStreamObserver<ConsumeOutput> consumeOutput = TestStreamObserver.create();
        StreamObserver<ConsumeInput> consumeInput = consumerStub.consume(consumeOutput);

        assertTrue(consumeOutput.takeOneMessage().hasSubscribeSuccess());

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer()
                .enableBatching(false)
                .topic("persistent://my-property/my-ns/my-topic1");

        Producer<byte[]> producer = producerBuilder.create();
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        CommandMessage message = null;
        Set<String> messageSet = Sets.newHashSet();
        for (int i = 0; i < 10; i++) {
            message = consumeOutput.takeOneMessage().getMessage();
            String receivedMessage = getPayload(message);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }
        // Acknowledge the consumption of all messages at once
        consumeInput.onNext(Commands.newAck(message.getMessageId(), AckType.Cumulative));
        Thread.sleep(100);
        consumeInput.onCompleted();
        consumeOutput.waitForCompletion();
        log.info("-- Exiting {} test --", methodName);
    }

    @Test
    public void testPulsarProducerAndGrpcMetadataAndPayloadUncompressedConsumer() throws Exception {
        log.info("-- Starting {} test --", methodName);

        // Lookup
        PulsarGrpc.PulsarBlockingStub blockingStub = PulsarGrpc.newBlockingStub(channel);
        blockingStub.lookupTopic(Commands.newLookup("persistent://my-property/my-ns/my-topic1", false));

        // Subscribe
        CommandSubscribe subscribe = Commands.newSubscribe("persistent://my-property/my-ns/my-topic1",
                "my-subscriber-name", CommandSubscribe.SubType.Exclusive, 0,
                "test", 0, PayloadType.METADATA_AND_PAYLOAD_UNCOMPRESSED);
        PulsarGrpc.PulsarStub consumerStub = Commands.attachConsumerParams(stub, subscribe);

        TestStreamObserver<ConsumeOutput> consumeOutput = TestStreamObserver.create();
        StreamObserver<ConsumeInput> consumeInput = consumerStub.consume(consumeOutput);

        assertTrue(consumeOutput.takeOneMessage().hasSubscribeSuccess());

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer()
                .enableBatching(false)
                .compressionType(org.apache.pulsar.client.api.CompressionType.LZ4)
                .topic("persistent://my-property/my-ns/my-topic1");

        Producer<byte[]> producer = producerBuilder.create();
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        CommandMessage message = null;
        Set<String> messageSet = Sets.newHashSet();
        for (int i = 0; i < 10; i++) {
            message = consumeOutput.takeOneMessage().getMessage();
            String receivedMessage = getPayload(message);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }
        // Acknowledge the consumption of all messages at once
        consumeInput.onNext(Commands.newAck(message.getMessageId(), AckType.Cumulative));
        Thread.sleep(100);
        consumeInput.onCompleted();
        consumeOutput.waitForCompletion();
        log.info("-- Exiting {} test --", methodName);
    }

    @Test
    public void testNonBatchedPulsarProducerAndGrpcBatchMessagesConsumer() throws Exception {
        log.info("-- Starting {} test --", methodName);

        // Lookup
        PulsarGrpc.PulsarBlockingStub blockingStub = PulsarGrpc.newBlockingStub(channel);
        blockingStub.lookupTopic(Commands.newLookup("persistent://my-property/my-ns/my-topic1", false));

        // Subscribe
        CommandSubscribe subscribe = Commands.newSubscribe("persistent://my-property/my-ns/my-topic1",
                "my-subscriber-name", CommandSubscribe.SubType.Exclusive, 0,
                "test", 0, PayloadType.MESSAGES);
        PulsarGrpc.PulsarStub consumerStub = Commands.attachConsumerParams(stub, subscribe);

        TestStreamObserver<ConsumeOutput> consumeOutput = TestStreamObserver.create();
        StreamObserver<ConsumeInput> consumeInput = consumerStub.consume(consumeOutput);

        assertTrue(consumeOutput.takeOneMessage().hasSubscribeSuccess());

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer()
                .enableBatching(false)
                .topic("persistent://my-property/my-ns/my-topic1");

        Producer<byte[]> producer = producerBuilder.create();
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        CommandMessage message = null;
        Set<String> messageSet = Sets.newHashSet();
        for (int i = 0; i < 10; i++) {
            message = consumeOutput.takeOneMessage().getMessage();
            String receivedMessage = getFirstPayloadInBatch(message);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }
        // Acknowledge the consumption of all messages at once
        consumeInput.onNext(Commands.newAck(message.getMessageId(), AckType.Cumulative));
        Thread.sleep(100);
        consumeInput.onCompleted();
        consumeOutput.waitForCompletion();
        log.info("-- Exiting {} test --", methodName);
    }

    @Test
    public void testBatchedPulsarProducerAndGrpcBatchMessagesConsumer() throws Exception {
        log.info("-- Starting {} test --", methodName);

        // Lookup
        PulsarGrpc.PulsarBlockingStub blockingStub = PulsarGrpc.newBlockingStub(channel);
        blockingStub.lookupTopic(Commands.newLookup("persistent://my-property/my-ns/my-topic1", false));

        // Subscribe
        CommandSubscribe subscribe = Commands.newSubscribe("persistent://my-property/my-ns/my-topic1",
                "my-subscriber-name", CommandSubscribe.SubType.Exclusive, 0,
                "test", 0, PayloadType.MESSAGES);
        PulsarGrpc.PulsarStub consumerStub = Commands.attachConsumerParams(stub, subscribe);

        TestStreamObserver<ConsumeOutput> consumeOutput = TestStreamObserver.create();
        StreamObserver<ConsumeInput> consumeInput = consumerStub.consume(consumeOutput);

        assertTrue(consumeOutput.takeOneMessage().hasSubscribeSuccess());

        // Send flow permits
        //consumeInput.onNext(Commands.newFlow(100));

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer()
                .enableBatching(true)
                .topic("persistent://my-property/my-ns/my-topic1");

        Producer<byte[]> producer = producerBuilder.create();
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.sendAsync(message.getBytes());
        }

        CommandMessage message = null;
        Set<String> messageSet = Sets.newHashSet();
        List<String> receivedMessages = new ArrayList<>();
        while (receivedMessages.size() != 10) {
            message = consumeOutput.takeOneMessage().getMessage();
            System.out.println(message.getMessages().getMessagesList().size());
            receivedMessages.addAll(getBatchPayloads(message));
        }

        for (int i = 0; i < 10; i++) {
            String receivedMessage = receivedMessages.get(i);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }
        // Acknowledge the consumption of all messages at once
        consumeInput.onNext(Commands.newAck(message.getMessageId(), AckType.Cumulative));
        Thread.sleep(100);
        consumeInput.onCompleted();
        consumeOutput.waitForCompletion();

        log.info("-- Exiting {} test --", methodName);
    }

    @Test
    public void testEncryptedPulsarProducerAndGrpcBatchMessagesConsumer() throws Exception {
        log.info("-- Starting {} test --", methodName);

        // Lookup
        PulsarGrpc.PulsarBlockingStub blockingStub = PulsarGrpc.newBlockingStub(channel);
        blockingStub.lookupTopic(Commands.newLookup("persistent://my-property/my-ns/my-topic1", false));
        blockingStub.lookupTopic(Commands.newLookup("persistent://my-property/my-ns/my-topic2", false));

        // Subscribe
        CommandSubscribe subscribe = Commands.newSubscribe("persistent://my-property/my-ns/my-topic1",
                "my-subscriber-name", CommandSubscribe.SubType.Exclusive, 0,
                "test", 0, PayloadType.MESSAGES);
        PulsarGrpc.PulsarStub consumerStub = Commands.attachConsumerParams(stub, subscribe);

        TestStreamObserver<ConsumeOutput> consumeOutput = TestStreamObserver.create();
        StreamObserver<ConsumeInput> consumeInput = consumerStub.consume(consumeOutput);

        assertTrue(consumeOutput.takeOneMessage().hasSubscribeSuccess());

        EncKeyReader reader = new EncKeyReader();

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer()
                .enableBatching(false)
                .addEncryptionKey("client-ecdsa.pem")
                .cryptoKeyReader(reader)
                .topic("persistent://my-property/my-ns/my-topic1");

        Producer<byte[]> producer = producerBuilder.create();
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        // Consumer of second topic
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic("persistent://my-property/my-ns/my-topic2")
                .cryptoKeyReader(reader)
                .subscriptionName("my-subscriber-name2")
                .subscribe();

        // Producer to second topic
        CommandProducer grpcProducer = Commands.newProducer("persistent://my-property/my-ns/my-topic2",
                "test", Collections.emptyMap());

        PulsarGrpc.PulsarStub producerStub = Commands.attachProducerParams(stub, grpcProducer);
        TestStreamObserver<SendResult> sendResult = TestStreamObserver.create();
        StreamObserver<CommandSend> commandSend = producerStub.produce(sendResult);

        assertTrue(sendResult.takeOneMessage().hasProducerSuccess());

        CommandMessage message = null;
        for (int i = 0; i < 10; i++) {
            message = consumeOutput.takeOneMessage().getMessage();
            assertTrue(message.hasMetadataAndPayload());
            // Proxy encrypted messages from topic1 to topic2 as-is
            commandSend.onNext(CommandSend.newBuilder()
                    .setMetadataAndPayload(message.getMetadataAndPayload())
                    .build());
        }
        // Acknowledge the consumption of all messages at once
        consumeInput.onNext(Commands.newAck(message.getMessageId(), AckType.Cumulative));
        Thread.sleep(100);
        consumeInput.onCompleted();
        consumeOutput.waitForCompletion();

        Message<byte[]> msg = null;
        Set<String> messageSet = Sets.newHashSet();
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            log.debug("Received message: [{}]", receivedMessage);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }
        commandSend.onCompleted();
        sendResult.waitForCompletion();

        // Acknowledge the consumption of all messages at once
        consumer.acknowledgeCumulative(msg);
        consumer.close();

        log.info("-- Exiting {} test --", methodName);
    }

    @Test
    public void testGrpcProducerBinaryPayloadAndPulsarConsumer() throws Exception {
        log.info("-- Starting {} test --", methodName);

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic("persistent://my-property/my-ns/my-topic1")
                .subscriptionName("my-subscriber-name").subscribe();

        CommandProducer producer = Commands.newProducer("persistent://my-property/my-ns/my-topic1",
                "test", Collections.emptyMap());

        PulsarGrpc.PulsarStub producerStub = Commands.attachProducerParams(stub, producer);
        TestStreamObserver<SendResult> sendResult = TestStreamObserver.create();
        StreamObserver<CommandSend> commandSend = producerStub.produce(sendResult);

        assertTrue(sendResult.takeOneMessage().hasProducerSuccess());

        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            org.apache.pulsar.common.api.proto.MessageMetadata messageMetadata =
                new org.apache.pulsar.common.api.proto.MessageMetadata()
                    .setPublishTime(System.currentTimeMillis())
                    .setProducerName("prod-name")
                    .setSequenceId(0);
            ByteBuf data = Unpooled.wrappedBuffer(message.getBytes());
            commandSend.onNext(Commands.newSend(i, 1, messageMetadata, data));
        }

        for (int i = 0; i < 10; i++) {
            assertTrue(sendResult.takeOneMessage().hasSendReceipt());
        }

        commandSend.onCompleted();
        sendResult.waitForCompletion();

        Message<byte[]> msg = null;
        Set<String> messageSet = Sets.newHashSet();
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            log.debug("Received message: [{}]", receivedMessage);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }
        // Acknowledge the consumption of all messages at once
        consumer.acknowledgeCumulative(msg);
        consumer.close();
        log.info("-- Exiting {} test --", methodName);
    }

    @Test
    public void testGrpcProducerSinglePayloadAndPulsarConsumer() throws Exception {
        log.info("-- Starting {} test --", methodName);

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic("persistent://my-property/my-ns/my-topic1")
                .subscriptionName("my-subscriber-name").subscribe();

        CommandProducer producer = Commands.newProducer("persistent://my-property/my-ns/my-topic1",
                "test", Collections.emptyMap());

        PulsarGrpc.PulsarStub producerStub = Commands.attachProducerParams(stub, producer);
        TestStreamObserver<SendResult> sendResult = TestStreamObserver.create();
        StreamObserver<CommandSend> commandSend = producerStub.produce(sendResult);

        assertTrue(sendResult.takeOneMessage().hasProducerSuccess());

        for (int i = 0; i < 10; i++) {
            CommandSend.Builder builder = CommandSend.newBuilder()
                    .setSequenceId(i)
                    .setMetadataAndPayload(MetadataAndPayload.newBuilder()
                            .setMetadata(MessageMetadata.newBuilder()
                                    .setPublishTime(System.currentTimeMillis())
                                    .setProducerName("prod-name")
                                    .setSequenceId(i))
                            .setPayload(ByteString.copyFromUtf8("my-message-" + i)));
            commandSend.onNext(builder.build());
        }

        for (int i = 0; i < 10; i++) {
            assertTrue(sendResult.takeOneMessage().hasSendReceipt());
        }

        commandSend.onCompleted();
        sendResult.waitForCompletion();

        Message<byte[]> msg = null;
        Set<String> messageSet = Sets.newHashSet();
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            log.debug("Received message: [{}]", receivedMessage);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }
        // Acknowledge the consumption of all messages at once
        consumer.acknowledgeCumulative(msg);
        consumer.close();
        log.info("-- Exiting {} test --", methodName);
    }

    @Test
    public void testGrpcProducerSinglePayloadCompressAndPulsarConsumer() throws Exception {
        log.info("-- Starting {} test --", methodName);

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic("persistent://my-property/my-ns/my-topic1")
                .subscriptionName("my-subscriber-name").subscribe();

        CommandProducer producer = Commands.newProducer("persistent://my-property/my-ns/my-topic1",
                "test", Collections.emptyMap());

        PulsarGrpc.PulsarStub producerStub = Commands.attachProducerParams(stub, producer);
        TestStreamObserver<SendResult> sendResult = TestStreamObserver.create();
        StreamObserver<CommandSend> commandSend = producerStub.produce(sendResult);

        assertTrue(sendResult.takeOneMessage().hasProducerSuccess());

        for (int i = 0; i < 10; i++) {
            CommandSend.Builder builder = CommandSend.newBuilder()
                    .setSequenceId(i)
                    .setMetadataAndPayload(MetadataAndPayload.newBuilder()
                            .setCompress(true)
                            .setMetadata(MessageMetadata.newBuilder()
                                    .setPublishTime(System.currentTimeMillis())
                                    .setProducerName("prod-name")
                                    .setCompression(CompressionType.LZ4)
                                    .setSequenceId(i))
                            .setPayload(ByteString.copyFromUtf8("my-message-" + i)));
            commandSend.onNext(builder.build());
        }

        for (int i = 0; i < 10; i++) {
            assertTrue(sendResult.takeOneMessage().hasSendReceipt());
        }

        commandSend.onCompleted();
        sendResult.waitForCompletion();

        Message<byte[]> msg = null;
        Set<String> messageSet = Sets.newHashSet();
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            log.debug("Received message: [{}]", receivedMessage);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }
        // Acknowledge the consumption of all messages at once
        consumer.acknowledgeCumulative(msg);
        consumer.close();
        log.info("-- Exiting {} test --", methodName);
    }

    @Test
    public void testGrpcProducerMultipleMessagesAndPulsarConsumer() throws Exception {
        log.info("-- Starting {} test --", methodName);

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic("persistent://my-property/my-ns/my-topic1")
                .subscriptionName("my-subscriber-name").subscribe();

        CommandProducer producer = Commands.newProducer("persistent://my-property/my-ns/my-topic1",
                "test", Collections.emptyMap());

        PulsarGrpc.PulsarStub producerStub = Commands.attachProducerParams(stub, producer);
        TestStreamObserver<SendResult> sendResult = TestStreamObserver.create();
        StreamObserver<CommandSend> commandSend = producerStub.produce(sendResult);

        assertTrue(sendResult.takeOneMessage().hasProducerSuccess());

        Messages.Builder messagesBuilder = Messages.newBuilder();
        for (int i = 0; i < 10; i++) {
            ByteString message = ByteString.copyFromUtf8("my-message-" + i);
            SingleMessage.Builder singleMessage = SingleMessage.newBuilder().setPayload(message);
            messagesBuilder.addMessages(singleMessage);
        }

        messagesBuilder.setMetadata(MessageMetadata.newBuilder()
                .setSequenceId(0)
                .setPublishTime(System.currentTimeMillis())
                .setProducerName("prod-name")
                .setHighestSequenceId(9));

        CommandSend.Builder builder = CommandSend.newBuilder()
                .setMessages(messagesBuilder);
        commandSend.onNext(builder.build());

        assertTrue(sendResult.takeOneMessage().hasSendReceipt());

        commandSend.onCompleted();
        sendResult.waitForCompletion();

        Message<byte[]> msg = null;
        Set<String> messageSet = Sets.newHashSet();
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            log.debug("Received message: [{}]", receivedMessage);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }
        // Acknowledge the consumption of all messages at once
        consumer.acknowledgeCumulative(msg);
        consumer.close();
        log.info("-- Exiting {} test --", methodName);
    }

    @Test
    public void testGrpcProducerMultipleMessagesCompressedAndPulsarConsumer() throws Exception {
        log.info("-- Starting {} test --", methodName);

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic("persistent://my-property/my-ns/my-topic1")
                .subscriptionName("my-subscriber-name").subscribe();

        CommandProducer producer = Commands.newProducer("persistent://my-property/my-ns/my-topic1",
                "test", Collections.emptyMap());

        PulsarGrpc.PulsarStub producerStub = Commands.attachProducerParams(stub, producer);
        TestStreamObserver<SendResult> sendResult = TestStreamObserver.create();
        StreamObserver<CommandSend> commandSend = producerStub.produce(sendResult);

        assertTrue(sendResult.takeOneMessage().hasProducerSuccess());

        Messages.Builder messagesBuilder = Messages.newBuilder();
        for (int i = 0; i < 10; i++) {
            ByteString message = ByteString.copyFromUtf8("my-message-" + i);
            SingleMessage.Builder singleMessage = SingleMessage.newBuilder().setPayload(message);
            messagesBuilder.addMessages(singleMessage);
        }

        messagesBuilder.setMetadata(MessageMetadata.newBuilder()
                .setSequenceId(0)
                .setPublishTime(System.currentTimeMillis())
                .setCompression(CompressionType.LZ4)
                .setProducerName("prod-name")
                .setHighestSequenceId(9));

        CommandSend.Builder builder = CommandSend.newBuilder()
                .setMessages(messagesBuilder);
        commandSend.onNext(builder.build());

        assertTrue(sendResult.takeOneMessage().hasSendReceipt());

        commandSend.onCompleted();
        sendResult.waitForCompletion();

        Message<byte[]> msg = null;
        Set<String> messageSet = Sets.newHashSet();
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            log.debug("Received message: [{}]", receivedMessage);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }
        // Acknowledge the consumption of all messages at once
        consumer.acknowledgeCumulative(msg);
        consumer.close();
        log.info("-- Exiting {} test --", methodName);
    }

    @Test
    public void testRedeliverUnacknowledgedMessages() throws Exception {
        log.info("-- Starting {} test --", methodName);

        // Lookup
        PulsarGrpc.PulsarBlockingStub blockingStub = PulsarGrpc.newBlockingStub(channel);
        blockingStub.lookupTopic(Commands.newLookup("persistent://my-property/my-ns/my-topic1", false));

        // Subscribe
        CommandSubscribe subscribe = Commands.newSubscribe("persistent://my-property/my-ns/my-topic1",
                "my-subscriber-name", CommandSubscribe.SubType.Exclusive, 0,
                "test", 0);
        PulsarGrpc.PulsarStub consumerStub = Commands.attachConsumerParams(stub, subscribe);

        TestStreamObserver<ConsumeOutput> consumeOutput = TestStreamObserver.create();
        StreamObserver<ConsumeInput> consumeInput = consumerStub.consume(consumeOutput);

        assertTrue(consumeOutput.takeOneMessage().hasSubscribeSuccess());

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer()
                .enableBatching(false)
                .topic("persistent://my-property/my-ns/my-topic1");

        Producer<byte[]> producer = producerBuilder.create();
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        Set<String> messageSet = Sets.newHashSet();
        for (int i = 0; i < 10; i++) {
            CommandMessage message = consumeOutput.takeOneMessage().getMessage();
            String receivedMessage = getFirstPayloadInBatch(message);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }

        // Redeliver all messages
        consumeInput.onNext(Commands.newRedeliverUnacknowledgedMessages());

        CommandMessage message = null;
        messageSet = Sets.newHashSet();
        for (int i = 0; i < 10; i++) {
            message = consumeOutput.takeOneMessage().getMessage();
            String receivedMessage = getFirstPayloadInBatch(message);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }
        // Acknowledge the consumption of all messages at once
        consumeInput.onNext(Commands.newAck(message.getMessageId(), AckType.Cumulative));
        Thread.sleep(100);
        consumeInput.onCompleted();
        consumeOutput.waitForCompletion();
        log.info("-- Exiting {} test --", methodName);
    }

    @Test
    public void testRedeliverSingleUnacknowledgedMessage() throws Exception {
        log.info("-- Starting {} test --", methodName);

        // Lookup
        PulsarGrpc.PulsarBlockingStub blockingStub = PulsarGrpc.newBlockingStub(channel);
        blockingStub.lookupTopic(Commands.newLookup("persistent://my-property/my-ns/my-topic1", false));

        // Subscribe
        CommandSubscribe subscribe = Commands.newSubscribe("persistent://my-property/my-ns/my-topic1",
                "my-subscriber-name", CommandSubscribe.SubType.Shared, 0,
                "test", 0);
        PulsarGrpc.PulsarStub consumerStub = Commands.attachConsumerParams(stub, subscribe);

        TestStreamObserver<ConsumeOutput> consumeOutput = TestStreamObserver.create();
        StreamObserver<ConsumeInput> consumeInput = consumerStub.consume(consumeOutput);

        assertTrue(consumeOutput.takeOneMessage().hasSubscribeSuccess());

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer()
                .enableBatching(false)
                .topic("persistent://my-property/my-ns/my-topic1");

        Producer<byte[]> producer = producerBuilder.create();
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        CommandMessage message = null;
        Set<String> messageSet = Sets.newHashSet();
        for (int i = 0; i < 10; i++) {
            message = consumeOutput.takeOneMessage().getMessage();
            String receivedMessage = getFirstPayloadInBatch(message);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }

        // Redeliver last message
        consumeInput
                .onNext(Commands.newRedeliverUnacknowledgedMessages(Collections.singletonList(message.getMessageId())));

        message = consumeOutput.takeOneMessage().getMessage();
        String receivedMessage = getFirstPayloadInBatch(message);
        assertEquals(receivedMessage, "my-message-9");

        // Acknowledge the consumption of all messages at once
        consumeInput.onNext(Commands.newAck(message.getMessageId(), AckType.Cumulative));
        Thread.sleep(100);
        consumeInput.onCompleted();
        consumeOutput.waitForCompletion();
        log.info("-- Exiting {} test --", methodName);
    }

    @Test
    public void testAckedMessagesAreNotRedelivered() throws Exception {
        log.info("-- Starting {} test --", methodName);

        // Lookup
        PulsarGrpc.PulsarBlockingStub blockingStub = PulsarGrpc.newBlockingStub(channel);
        blockingStub.lookupTopic(Commands.newLookup("persistent://my-property/my-ns/my-topic1", false));

        // Subscribe
        CommandSubscribe subscribe = Commands.newSubscribe("persistent://my-property/my-ns/my-topic1",
                "my-subscriber-name", CommandSubscribe.SubType.Exclusive, 0,
                "test", 0);
        PulsarGrpc.PulsarStub consumerStub = Commands.attachConsumerParams(stub, subscribe);

        TestStreamObserver<ConsumeOutput> consumeOutput = TestStreamObserver.create();
        StreamObserver<ConsumeInput> consumeInput = consumerStub.consume(consumeOutput);

        assertTrue(consumeOutput.takeOneMessage().hasSubscribeSuccess());

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer()
                .enableBatching(false)
                .topic("persistent://my-property/my-ns/my-topic1");

        Producer<byte[]> producer = producerBuilder.create();
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        Set<String> messageSet = Sets.newHashSet();
        for (int i = 0; i < 10; i++) {
            CommandMessage message = consumeOutput.takeOneMessage().getMessage();
            String receivedMessage = getFirstPayloadInBatch(message);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
            if (i == 4) {
                // Acknowledge the consumption up to this message
                consumeInput.onNext(Commands.newAck(message.getMessageId(), AckType.Cumulative));
            }
        }

        // Redeliver all unacknowledged messages
        consumeInput.onNext(Commands.newRedeliverUnacknowledgedMessages());

        CommandMessage message = null;
        messageSet = Sets.newHashSet();
        // Verify that only the last 5 messages are redelivered
        for (int i = 5; i < 10; i++) {
            message = consumeOutput.takeOneMessage().getMessage();
            String receivedMessage = getFirstPayloadInBatch(message);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }

        // Acknowledge the consumption of all messages at once
        consumeInput.onNext(Commands.newAck(message.getMessageId(), AckType.Cumulative));
        Thread.sleep(100);
        consumeInput.onCompleted();
        consumeOutput.waitForCompletion();
        log.info("-- Exiting {} test --", methodName);
    }

    @Test
    public void testGetLastMessageId() throws Exception {
        log.info("-- Starting {} test --", methodName);

        // Lookup
        PulsarGrpc.PulsarBlockingStub blockingStub = PulsarGrpc.newBlockingStub(channel);
        blockingStub.lookupTopic(Commands.newLookup("persistent://my-property/my-ns/my-topic1", false));

        // Subscribe
        CommandSubscribe subscribe = Commands.newSubscribe("persistent://my-property/my-ns/my-topic1",
                "my-subscriber-name", CommandSubscribe.SubType.Exclusive, 0,
                "test", 0);
        PulsarGrpc.PulsarStub consumerStub = Commands.attachConsumerParams(stub, subscribe);

        TestStreamObserver<ConsumeOutput> consumeOutput = TestStreamObserver.create();
        StreamObserver<ConsumeInput> consumeInput = consumerStub.consume(consumeOutput);

        assertTrue(consumeOutput.takeOneMessage().hasSubscribeSuccess());

        // Send flow permits
        //consumeInput.onNext(Commands.newFlow(100));

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer()
                .enableBatching(false)
                .topic("persistent://my-property/my-ns/my-topic1");

        Producer<byte[]> producer = producerBuilder.create();
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        CommandMessage message = null;
        for (int i = 0; i < 10; i++) {
            message = consumeOutput.takeOneMessage().getMessage();
        }
        // Acknowledge the consumption of all messages at once
        consumeInput.onNext(Commands.newAck(message.getMessageId(), AckType.Cumulative));

        consumeInput.onNext(Commands.newGetLastMessageId(1));
        ConsumeOutput output = consumeOutput.takeOneMessage();
        assertTrue(output.hasGetLastMessageIdResponse());
        CommandGetLastMessageIdResponse response = output.getGetLastMessageIdResponse();
        assertEquals(response.getRequestId(), 1);
        assertEquals(response.getLastMessageId().getLedgerId(), 3);
        assertEquals(response.getLastMessageId().getEntryId(), 9);
        assertEquals(response.getLastMessageId().getPartition(), -1);
        assertEquals(response.getLastMessageId().getBatchIndex(), -1);

        consumeInput.onCompleted();
        consumeOutput.waitForCompletion();
        log.info("-- Exiting {} test --", methodName);
    }

    @Test
    public void testGetLastMessageIdNotProducedYet() throws Exception {
        log.info("-- Starting {} test --", methodName);

        // Lookup
        PulsarGrpc.PulsarBlockingStub blockingStub = PulsarGrpc.newBlockingStub(channel);
        blockingStub.lookupTopic(Commands.newLookup("persistent://my-property/my-ns/my-topic1", false));

        // Subscribe
        CommandSubscribe subscribe = Commands.newSubscribe("persistent://my-property/my-ns/my-topic1",
                "my-subscriber-name", CommandSubscribe.SubType.Exclusive, 0,
                "test", 0);
        PulsarGrpc.PulsarStub consumerStub = Commands.attachConsumerParams(stub, subscribe);

        TestStreamObserver<ConsumeOutput> consumeOutput = TestStreamObserver.create();
        StreamObserver<ConsumeInput> consumeInput = consumerStub.consume(consumeOutput);

        assertTrue(consumeOutput.takeOneMessage().hasSubscribeSuccess());

        consumeInput.onNext(Commands.newGetLastMessageId(1));
        ConsumeOutput output = consumeOutput.takeOneMessage();
        assertTrue(output.hasGetLastMessageIdResponse());
        CommandGetLastMessageIdResponse response = output.getGetLastMessageIdResponse();
        assertEquals(response.getRequestId(), 1);
        assertEquals(response.getLastMessageId().getLedgerId(), 3);
        assertEquals(response.getLastMessageId().getEntryId(), -1);
        assertEquals(response.getLastMessageId().getPartition(), -1);
        assertEquals(response.getLastMessageId().getBatchIndex(), -1);

        consumeInput.onCompleted();
        consumeOutput.waitForCompletion();
        log.info("-- Exiting {} test --", methodName);
    }

    @Test
    public void testSubscriptionSeekByMessageId() throws Exception {
        log.info("-- Starting {} test --", methodName);

        // Lookup
        PulsarGrpc.PulsarBlockingStub blockingStub = PulsarGrpc.newBlockingStub(channel);
        blockingStub.lookupTopic(Commands.newLookup("persistent://my-property/my-ns/my-topic1", false));

        // Subscribe
        CommandSubscribe subscribe = Commands.newSubscribe("persistent://my-property/my-ns/my-topic1",
                "my-subscriber-name", CommandSubscribe.SubType.Exclusive, 0,
                "test", 0);
        PulsarGrpc.PulsarStub consumerStub = Commands.attachConsumerParams(stub, subscribe);

        TestStreamObserver<ConsumeOutput> consumeOutput = TestStreamObserver.create();
        StreamObserver<ConsumeInput> consumeInput = consumerStub.consume(consumeOutput);

        assertTrue(consumeOutput.takeOneMessage().hasSubscribeSuccess());

        // Send flow permits
        //consumeInput.onNext(Commands.newFlow(100));

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer()
                .enableBatching(false)
                .topic("persistent://my-property/my-ns/my-topic1");

        Producer<byte[]> producer = producerBuilder.create();
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        CommandMessage message = null;
        CommandMessage seekMessage = null;
        Set<String> messageSet = Sets.newHashSet();
        for (int i = 0; i < 10; i++) {
            message = consumeOutput.takeOneMessage().getMessage();
            String receivedMessage = getFirstPayloadInBatch(message);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
            if (i == 5) {
                seekMessage = message;
            }
        }
        // Acknowledge the consumption of all messages at once
        consumeInput.onNext(Commands.newAck(message.getMessageId(), AckType.Cumulative));

        MessageIdData messageId = seekMessage.getMessageId();
        consumeInput.onNext(Commands.newSeek(1, messageId.getLedgerId(), messageId.getEntryId(), new long[0]));

        // At the moment the consumer is disconnected during a seek. So we reconnect
        // See https://github.com/apache/pulsar/issues/5073
        consumeOutput.waitForCompletion();
        Thread.sleep(100);
        consumeInput = consumerStub.consume(consumeOutput);
        assertTrue(consumeOutput.takeOneMessage().hasSubscribeSuccess());

        // Send flow permits
        //consumeInput.onNext(Commands.newFlow(100));

        messageSet = Sets.newHashSet();
        for (int i = 5; i < 10; i++) {
            message = consumeOutput.takeOneMessage().getMessage();
            String receivedMessage = getFirstPayloadInBatch(message);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }

        // Acknowledge the consumption of all messages at once
        consumeInput.onNext(Commands.newAck(message.getMessageId(), AckType.Cumulative));
        Thread.sleep(100);

        consumeInput.onCompleted();
        consumeOutput.waitForCompletion();
        log.info("-- Exiting {} test --", methodName);
    }

    @Test
    public void testSubscriptionSeekByTimestamp() throws Exception {
        log.info("-- Starting {} test --", methodName);

        // Lookup
        PulsarGrpc.PulsarBlockingStub blockingStub = PulsarGrpc.newBlockingStub(channel);
        blockingStub.lookupTopic(Commands.newLookup("persistent://my-property/my-ns/my-topic1", false));

        // Subscribe
        CommandSubscribe subscribe = Commands.newSubscribe("persistent://my-property/my-ns/my-topic1",
                "my-subscriber-name", CommandSubscribe.SubType.Exclusive, 0,
                "test", 0);
        PulsarGrpc.PulsarStub consumerStub = Commands.attachConsumerParams(stub, subscribe);

        TestStreamObserver<ConsumeOutput> consumeOutput = TestStreamObserver.create();
        StreamObserver<ConsumeInput> consumeInput = consumerStub.consume(consumeOutput);

        assertTrue(consumeOutput.takeOneMessage().hasSubscribeSuccess());

        // Send flow permits
        //consumeInput.onNext(Commands.newFlow(100));

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer()
                .enableBatching(false)
                .topic("persistent://my-property/my-ns/my-topic1");

        Producer<byte[]> producer = producerBuilder.create();
        long timestamp = Long.MAX_VALUE;
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            if (i == 5) {
                timestamp = System.currentTimeMillis();
            }
            producer.send(message.getBytes());
            Thread.sleep(10);
        }

        CommandMessage message = null;
        CommandMessage seekMessage = null;
        Set<String> messageSet = Sets.newHashSet();
        for (int i = 0; i < 10; i++) {
            message = consumeOutput.takeOneMessage().getMessage();
            String receivedMessage = getFirstPayloadInBatch(message);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
            if (i == 5) {
                seekMessage = message;
            }
        }
        // Acknowledge the consumption of all messages at once
        consumeInput.onNext(Commands.newAck(message.getMessageId(), AckType.Cumulative));

        MessageIdData messageId = seekMessage.getMessageId();
        consumeInput.onNext(Commands.newSeek(1, timestamp));

        // At the moment the consumer is disconnected during a seek. So we reconnect
        // See https://github.com/apache/pulsar/issues/5073
        consumeOutput.waitForCompletion();
        Thread.sleep(100);
        consumeInput = consumerStub.consume(consumeOutput);
        assertTrue(consumeOutput.takeOneMessage().hasSubscribeSuccess());

        // Send flow permits
        //consumeInput.onNext(Commands.newFlow(100));

        messageSet = Sets.newHashSet();
        for (int i = 5; i < 10; i++) {
            message = consumeOutput.takeOneMessage().getMessage();
            String receivedMessage = getFirstPayloadInBatch(message);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }

        // Acknowledge the consumption of all messages at once
        consumeInput.onNext(Commands.newAck(message.getMessageId(), AckType.Cumulative));
        Thread.sleep(100);

        consumeInput.onCompleted();
        consumeOutput.waitForCompletion();
        log.info("-- Exiting {} test --", methodName);
    }

    @Test
    public void testLookupTopic() throws Exception {
        String topic = "persistent://my-property/my-ns/my-topic1";
        admin.topics().createNonPartitionedTopic(topic);

        CommandLookupTopic lookupTopic = CommandLookupTopic.newBuilder().setTopic(topic).build();

        CommandLookupTopicResponse topicResponse = blockingStub.lookupTopic(lookupTopic);

        assertEquals(topicResponse.getGrpcServiceHost(), "localhost");
        assertEquals(topicResponse.getGrpcServicePort(), (int) grpcService.getListenPort().orElse(null));
        assertEquals(topicResponse.getResponse(), LookupType.Connect);
    }

    @Test
    public void testGetPartitionedTopicMetadata() throws Exception {
        int numPartitions = 4;
        String topic = "persistent://my-property/my-ns/my-partitionedtopic1-" + System.currentTimeMillis();

        admin.topics().createPartitionedTopic(topic, numPartitions);

        CommandPartitionedTopicMetadata commandPartitionedTopicMetadata =
                CommandPartitionedTopicMetadata.newBuilder().setTopic(topic).build();

        CommandPartitionedTopicMetadataResponse response =
                blockingStub.getPartitionMetadata(commandPartitionedTopicMetadata);

        assertEquals(response.getPartitions(), numPartitions);
    }

    @Test
    public void testGetSchema() throws Exception {
        pulsarClient.newProducer(org.apache.pulsar.client.api.Schema.AVRO(V1Data.class))
                .topic("persistent://my-property/my-ns/my-topic1")
                .create();

        pulsarClient.newProducer(org.apache.pulsar.client.api.Schema.AVRO(V2Data.class))
                .topic("persistent://my-property/my-ns/my-topic1")
                .create();

        CommandGetSchema commandGetSchema = CommandGetSchema.newBuilder()
                .setTopic("persistent://my-property/my-ns/my-topic1")
                .build();

        CommandGetSchemaResponse schemaResponse = blockingStub.getSchema(commandGetSchema);

        assertEquals(schemaResponse.getSchema().getType(), Schema.Type.Avro);
        SchemaVersion schemaVersion = pulsar.getSchemaRegistryService()
                .versionFromBytes(schemaResponse.getSchemaVersion().toByteArray());
        assertEquals(schemaVersion, new LongSchemaVersion(1));
    }

    @Test
    public void testGetSchemaWithVersion() throws Exception {
        pulsarClient.newProducer(org.apache.pulsar.client.api.Schema.AVRO(V1Data.class))
                .topic("persistent://my-property/my-ns/my-topic1")
                .create();

        pulsarClient.newProducer(org.apache.pulsar.client.api.Schema.AVRO(V2Data.class))
                .topic("persistent://my-property/my-ns/my-topic1")
                .create();

        CommandGetSchema commandGetSchema =
                Commands.newGetSchema("persistent://my-property/my-ns/my-topic1", new LongSchemaVersion(0));

        CommandGetSchemaResponse schemaResponse = blockingStub.getSchema(commandGetSchema);

        assertEquals(schemaResponse.getSchema().getType(), Schema.Type.Avro);
        SchemaVersion schemaVersion = pulsar.getSchemaRegistryService()
                .versionFromBytes(schemaResponse.getSchemaVersion().toByteArray());
        assertEquals(schemaVersion, new LongSchemaVersion(0));
    }

    @Test
    public void testGetOrCreateSchema() throws Exception {
        admin.topics().createNonPartitionedTopic("persistent://my-property/my-ns/my-topic1");
        CommandGetOrCreateSchema getOrCreateSchema = Commands.newGetOrCreateSchema(
                "persistent://my-property/my-ns/my-topic1",
                org.apache.pulsar.client.api.Schema.STRING.getSchemaInfo());

        CommandGetOrCreateSchemaResponse response = blockingStub.getOrCreateSchema(getOrCreateSchema);

        SchemaVersion schemaVersion = pulsar.getSchemaRegistryService()
                .versionFromBytes(response.getSchemaVersion().toByteArray());
        assertEquals(schemaVersion, new LongSchemaVersion(0));
    }

    private static String getPayload(CommandMessage message) {
        assertTrue(message.hasMetadataAndPayload());
        String receivedMessage = message.getMetadataAndPayload().getPayload().toStringUtf8();
        log.info("Received messages: [{}]", receivedMessage);
        return receivedMessage;
    }

    private static List<String> getBatchPayloads(CommandMessage message) {
        assertTrue(message.hasMessages());
        List<String> receivedMessages = message.getMessages().getMessagesList().stream()
                .map(SingleMessage::getPayload)
                .map(ByteString::toStringUtf8)
                .collect(Collectors.toList());
        log.info("Received messages: [{}]", receivedMessages);
        return receivedMessages;
    }

    private static String getFirstPayloadInBatch(CommandMessage message) {
        List<String> payloads = getBatchPayloads(message);
        assertTrue(payloads.size() > 0);
        return payloads.get(0);
    }

    private static class TestStreamObserver<T> implements StreamObserver<T> {

        public static final int TIMEOUT = 10;

        public static <T> TestStreamObserver<T> create() {
            return new TestStreamObserver<>();
        }

        private final LinkedBlockingQueue<T> queue = new LinkedBlockingQueue<>();
        private final CompletableFuture<Throwable> error = new CompletableFuture<>();
        private final CountDownLatch complete = new CountDownLatch(1);

        private TestStreamObserver() {
        }

        @Override
        public void onNext(T value) {
            queue.add(value);
        }

        @Override
        public void onError(Throwable t) {
            error.complete(t);
        }

        @Override
        public void onCompleted() {
            complete.countDown();
        }

        public T takeOneMessage() throws InterruptedException, TimeoutException {
            T poll = queue.poll(TIMEOUT, TimeUnit.SECONDS);
            if (poll == null) {
                throw new TimeoutException("Timeout occurred while waiting message");
            }
            return poll;
        }

        public void waitForError() throws InterruptedException, TimeoutException, ExecutionException {
            error.get(TIMEOUT, TimeUnit.SECONDS);
        }

        public void waitForCompletion() throws InterruptedException {
            complete.await(TIMEOUT, TimeUnit.SECONDS);
        }

    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    static class V1Data {
        int i;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    static class V2Data {
        int i;
        Integer j;
    }

    class EncKeyReader implements CryptoKeyReader {
        EncryptionKeyInfo keyInfo = new EncryptionKeyInfo();

        EncKeyReader() {
        }

        public EncryptionKeyInfo getPublicKey(String keyName, Map<String, String> keyMeta) {
            String certFilePath = "./src/test/resources/certificate/public-key." + keyName;
            if (Files.isReadable(Paths.get(certFilePath))) {
                try {
                    this.keyInfo.setKey(Files.readAllBytes(Paths.get(certFilePath)));
                    return this.keyInfo;
                } catch (IOException var5) {
                    Assert.fail("Failed to read certificate from " + certFilePath);
                }
            } else {
                Assert.fail("Certificate file " + certFilePath + " is not present or not readable.");
            }

            return null;
        }

        public EncryptionKeyInfo getPrivateKey(String keyName, Map<String, String> keyMeta) {
            String certFilePath = "./src/test/resources/certificate/private-key." + keyName;
            if (Files.isReadable(Paths.get(certFilePath))) {
                try {
                    this.keyInfo.setKey(Files.readAllBytes(Paths.get(certFilePath)));
                    return this.keyInfo;
                } catch (IOException var5) {
                    Assert.fail("Failed to read certificate from " + certFilePath);
                }
            } else {
                Assert.fail("Certificate file " + certFilePath + " is not present or not readable.");
            }

            return null;
        }
    }

}
