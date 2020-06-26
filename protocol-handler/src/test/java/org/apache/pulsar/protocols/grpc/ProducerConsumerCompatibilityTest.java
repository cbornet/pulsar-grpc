package org.apache.pulsar.protocols.grpc;

import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
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
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.schema.LongSchemaVersion;
import org.apache.pulsar.protocols.grpc.api.CommandAck.AckType;
import org.apache.pulsar.protocols.grpc.api.CommandGetLastMessageIdResponse;
import org.apache.pulsar.protocols.grpc.api.CommandGetOrCreateSchema;
import org.apache.pulsar.protocols.grpc.api.CommandGetOrCreateSchemaResponse;
import org.apache.pulsar.protocols.grpc.api.CommandGetSchema;
import org.apache.pulsar.protocols.grpc.api.CommandGetSchemaResponse;
import org.apache.pulsar.protocols.grpc.api.CommandLookupTopic;
import org.apache.pulsar.protocols.grpc.api.CommandLookupTopicResponse;
import org.apache.pulsar.protocols.grpc.api.CommandLookupTopicResponse.LookupType;
import org.apache.pulsar.protocols.grpc.api.CommandMessage;
import org.apache.pulsar.protocols.grpc.api.CommandPartitionedTopicMetadata;
import org.apache.pulsar.protocols.grpc.api.CommandPartitionedTopicMetadataResponse;
import org.apache.pulsar.protocols.grpc.api.CommandProducer;
import org.apache.pulsar.protocols.grpc.api.CommandSend;
import org.apache.pulsar.protocols.grpc.api.CommandSubscribe;
import org.apache.pulsar.protocols.grpc.api.CompressionType;
import org.apache.pulsar.protocols.grpc.api.ConsumeInput;
import org.apache.pulsar.protocols.grpc.api.ConsumeOutput;
import org.apache.pulsar.protocols.grpc.api.MessageIdData;
import org.apache.pulsar.protocols.grpc.api.MessageMetadata;
import org.apache.pulsar.protocols.grpc.api.Messages;
import org.apache.pulsar.protocols.grpc.api.MetadataAndPayload;
import org.apache.pulsar.protocols.grpc.api.PayloadType;
import org.apache.pulsar.protocols.grpc.api.PulsarGrpc;
import org.apache.pulsar.protocols.grpc.api.Schema;
import org.apache.pulsar.protocols.grpc.api.SendResult;
import org.apache.pulsar.protocols.grpc.api.SingleMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.pulsar.common.protocol.Commands.parseMessageMetadata;
import static org.mockito.Mockito.doReturn;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class ProducerConsumerCompatibilityTest extends ProducerConsumerBase {

    private static final Logger log = LoggerFactory.getLogger(ProducerConsumerCompatibilityTest.class);

    private GrpcService grpcService;
    private PulsarGrpc.PulsarStub stub;
    private ManagedChannel channel;

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();

        grpcService = new GrpcService();
        conf.getProperties().setProperty("grpcServicePort", "0");
        grpcService.initialize(conf);
        grpcService.start(pulsar.getBrokerService());
        Map<String, String> protocolDataToAdvertise = new HashMap<>();
        protocolDataToAdvertise.put(grpcService.protocolName(), grpcService.getProtocolDataToAdvertise());
        doReturn(protocolDataToAdvertise).when(pulsar).getProtocolDataToAdvertise();
        pulsar.getLoadManager().get().stop();
        pulsar.getLoadManager().get().start();

        NettyChannelBuilder channelBuilder = NettyChannelBuilder
                .forAddress("localhost", grpcService.getListenPort().orElse(-1))
                .usePlaintext()
                .negotiationType(NegotiationType.PLAINTEXT);
        channel = channelBuilder.build();
        stub = PulsarGrpc.newStub(channel);
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
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
                "test" , 0, PayloadType.BINARY);
        PulsarGrpc.PulsarStub consumerStub = Commands.attachConsumerParams(stub, subscribe);

        TestStreamObserver<ConsumeOutput> consumeOutput = TestStreamObserver.create();
        StreamObserver<ConsumeInput> consumeInput = consumerStub.consume(consumeOutput);

        assertTrue(consumeOutput.takeOneMessage().hasSubscribeSuccess());

        // Send flow permits
        consumeInput.onNext(Commands.newFlow(100));

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
                "test" , 0, PayloadType.METADATA_AND_PAYLOAD);
        PulsarGrpc.PulsarStub consumerStub = Commands.attachConsumerParams(stub, subscribe);

        TestStreamObserver<ConsumeOutput> consumeOutput = TestStreamObserver.create();
        StreamObserver<ConsumeInput> consumeInput = consumerStub.consume(consumeOutput);

        assertTrue(consumeOutput.takeOneMessage().hasSubscribeSuccess());

        // Send flow permits
        consumeInput.onNext(Commands.newFlow(100));

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
                "test" , 0, PayloadType.METADATA_AND_PAYLOAD_UNCOMPRESSED);
        PulsarGrpc.PulsarStub consumerStub = Commands.attachConsumerParams(stub, subscribe);

        TestStreamObserver<ConsumeOutput> consumeOutput = TestStreamObserver.create();
        StreamObserver<ConsumeInput> consumeInput = consumerStub.consume(consumeOutput);

        assertTrue(consumeOutput.takeOneMessage().hasSubscribeSuccess());

        // Send flow permits
        consumeInput.onNext(Commands.newFlow(100));

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
            PulsarApi.MessageMetadata messageMetadata = PulsarApi.MessageMetadata.newBuilder()
                    .setPublishTime(System.currentTimeMillis())
                    .setProducerName("prod-name")
                    .setSequenceId(0)
                    .build();
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
                "test" , 0);
        PulsarGrpc.PulsarStub consumerStub = Commands.attachConsumerParams(stub, subscribe);

        TestStreamObserver<ConsumeOutput> consumeOutput = TestStreamObserver.create();
        StreamObserver<ConsumeInput> consumeInput = consumerStub.consume(consumeOutput);

        assertTrue(consumeOutput.takeOneMessage().hasSubscribeSuccess());

        // Send flow permits
        consumeInput.onNext(Commands.newFlow(100));

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
            String receivedMessage = getPayload(message);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }

        // Redeliver all messages
        consumeInput.onNext(Commands.newRedeliverUnacknowledgedMessages());

        CommandMessage message = null;
        messageSet = Sets.newHashSet();
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
    public void testRedeliverSingleUnacknowledgedMessage() throws Exception {
        log.info("-- Starting {} test --", methodName);

        // Lookup
        PulsarGrpc.PulsarBlockingStub blockingStub = PulsarGrpc.newBlockingStub(channel);
        blockingStub.lookupTopic(Commands.newLookup("persistent://my-property/my-ns/my-topic1", false));

        // Subscribe
        CommandSubscribe subscribe = Commands.newSubscribe("persistent://my-property/my-ns/my-topic1",
                "my-subscriber-name", CommandSubscribe.SubType.Shared, 0,
                "test" , 0);
        PulsarGrpc.PulsarStub consumerStub = Commands.attachConsumerParams(stub, subscribe);

        TestStreamObserver<ConsumeOutput> consumeOutput = TestStreamObserver.create();
        StreamObserver<ConsumeInput> consumeInput = consumerStub.consume(consumeOutput);

        assertTrue(consumeOutput.takeOneMessage().hasSubscribeSuccess());

        // Send flow permits
        consumeInput.onNext(Commands.newFlow(100));

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

        // Redeliver last message
        consumeInput.onNext(Commands.newRedeliverUnacknowledgedMessages(Collections.singletonList(message.getMessageId())));

        message = consumeOutput.takeOneMessage().getMessage();
        String receivedMessage = getPayload(message);
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
                "test" , 0);
        PulsarGrpc.PulsarStub consumerStub = Commands.attachConsumerParams(stub, subscribe);

        TestStreamObserver<ConsumeOutput> consumeOutput = TestStreamObserver.create();
        StreamObserver<ConsumeInput> consumeInput = consumerStub.consume(consumeOutput);

        assertTrue(consumeOutput.takeOneMessage().hasSubscribeSuccess());

        // Send flow permits
        consumeInput.onNext(Commands.newFlow(100));

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
            String receivedMessage = getPayload(message);
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
    public void testGetLastMessageId() throws Exception {
        log.info("-- Starting {} test --", methodName);

        // Lookup
        PulsarGrpc.PulsarBlockingStub blockingStub = PulsarGrpc.newBlockingStub(channel);
        blockingStub.lookupTopic(Commands.newLookup("persistent://my-property/my-ns/my-topic1", false));

        // Subscribe
        CommandSubscribe subscribe = Commands.newSubscribe("persistent://my-property/my-ns/my-topic1",
                "my-subscriber-name", CommandSubscribe.SubType.Exclusive, 0,
                "test" , 0);
        PulsarGrpc.PulsarStub consumerStub = Commands.attachConsumerParams(stub, subscribe);

        TestStreamObserver<ConsumeOutput> consumeOutput = TestStreamObserver.create();
        StreamObserver<ConsumeInput> consumeInput = consumerStub.consume(consumeOutput);

        assertTrue(consumeOutput.takeOneMessage().hasSubscribeSuccess());

        // Send flow permits
        consumeInput.onNext(Commands.newFlow(100));

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer()
                .enableBatching(true)
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
                "test" , 0);
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
                "test" , 0);
        PulsarGrpc.PulsarStub consumerStub = Commands.attachConsumerParams(stub, subscribe);

        TestStreamObserver<ConsumeOutput> consumeOutput = TestStreamObserver.create();
        StreamObserver<ConsumeInput> consumeInput = consumerStub.consume(consumeOutput);

        assertTrue(consumeOutput.takeOneMessage().hasSubscribeSuccess());

        // Send flow permits
        consumeInput.onNext(Commands.newFlow(100));

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
            String receivedMessage = getPayload(message);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
            if (i == 5) {
                seekMessage = message;
            }
        }
        // Acknowledge the consumption of all messages at once
        consumeInput.onNext(Commands.newAck(message.getMessageId(), AckType.Cumulative));

        MessageIdData messageId = seekMessage.getMessageId();
        consumeInput.onNext(Commands.newSeek(1, messageId.getLedgerId(), messageId.getEntryId()));

        // At the moment the consumer is disconnected during a seek. So we reconnect
        // See https://github.com/apache/pulsar/issues/5073
        consumeOutput.waitForCompletion();
        Thread.sleep(100);
        consumeInput = consumerStub.consume(consumeOutput);
        assertTrue(consumeOutput.takeOneMessage().hasSubscribeSuccess());

        // Send flow permits
        consumeInput.onNext(Commands.newFlow(100));

        messageSet = Sets.newHashSet();
        for (int i = 5; i < 10; i++) {
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
    public void testSubscriptionSeekByTimestamp() throws Exception {
        log.info("-- Starting {} test --", methodName);

        // Lookup
        PulsarGrpc.PulsarBlockingStub blockingStub = PulsarGrpc.newBlockingStub(channel);
        blockingStub.lookupTopic(Commands.newLookup("persistent://my-property/my-ns/my-topic1", false));

        // Subscribe
        CommandSubscribe subscribe = Commands.newSubscribe("persistent://my-property/my-ns/my-topic1",
                "my-subscriber-name", CommandSubscribe.SubType.Exclusive, 0,
                "test" , 0);
        PulsarGrpc.PulsarStub consumerStub = Commands.attachConsumerParams(stub, subscribe);

        TestStreamObserver<ConsumeOutput> consumeOutput = TestStreamObserver.create();
        StreamObserver<ConsumeInput> consumeInput = consumerStub.consume(consumeOutput);

        assertTrue(consumeOutput.takeOneMessage().hasSubscribeSuccess());

        // Send flow permits
        consumeInput.onNext(Commands.newFlow(100));

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
            String receivedMessage = getPayload(message);
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
        consumeInput.onNext(Commands.newFlow(100));

        messageSet = Sets.newHashSet();
        for (int i = 5; i < 10; i++) {
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
    public void testLookupTopic() throws Exception {
        String topic = "persistent://my-property/my-ns/my-topic1";
        admin.topics().createNonPartitionedTopic(topic);

        CommandLookupTopic lookupTopic = CommandLookupTopic.newBuilder().setTopic(topic).build();
        TestStreamObserver<CommandLookupTopicResponse> response = TestStreamObserver.create();

        stub.lookupTopic(lookupTopic, response);

        CommandLookupTopicResponse topicResponse = response.takeOneMessage();
        assertEquals(topicResponse.getGrpcServiceHost(), "localhost");
        assertEquals(topicResponse.getGrpcServicePort(), (int)grpcService.getListenPort().orElse(null));
        assertEquals(topicResponse.getResponse(), LookupType.Connect);

        response.waitForCompletion();
    }

    @Test
    public void testGetPartitionedTopicMetadata() throws Exception {
        int numPartitions = 4;
        String topic = "persistent://my-property/my-ns/my-partitionedtopic1-" + System.currentTimeMillis();

        admin.topics().createPartitionedTopic(topic, numPartitions);

        CommandPartitionedTopicMetadata commandPartitionedTopicMetadata =
                CommandPartitionedTopicMetadata.newBuilder().setTopic(topic).build();
        TestStreamObserver<CommandPartitionedTopicMetadataResponse> response = TestStreamObserver.create();

        stub.getPartitionMetadata(commandPartitionedTopicMetadata, response);

        assertEquals(response.takeOneMessage().getPartitions(), numPartitions);

        response.waitForCompletion();
    }

    @Test
    public void testGetSchema() throws Exception {
        pulsarClient.newProducer(org.apache.pulsar.client.api.Schema.AVRO(V1Data.class))
                .topic("persistent://my-property/my-ns/my-topic1")
                .create();

        pulsarClient.newProducer(org.apache.pulsar.client.api.Schema.AVRO(V2Data.class))
                .topic("persistent://my-property/my-ns/my-topic1")
                .create();
        TestStreamObserver<CommandGetSchemaResponse> response = TestStreamObserver.create();
        CommandGetSchema commandGetSchema = CommandGetSchema.newBuilder()
                .setTopic("persistent://my-property/my-ns/my-topic1")
                .build();

        stub.getSchema(commandGetSchema, response);
        CommandGetSchemaResponse schemaResponse = response.takeOneMessage();

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

        TestStreamObserver<CommandGetSchemaResponse> response = TestStreamObserver.create();
        CommandGetSchema commandGetSchema =
                Commands.newGetSchema("persistent://my-property/my-ns/my-topic1", new LongSchemaVersion(0));

        stub.getSchema(commandGetSchema, response);
        CommandGetSchemaResponse schemaResponse = response.takeOneMessage();

        assertEquals(schemaResponse.getSchema().getType(), Schema.Type.Avro);
        SchemaVersion schemaVersion = pulsar.getSchemaRegistryService()
                .versionFromBytes(schemaResponse.getSchemaVersion().toByteArray());
        assertEquals(schemaVersion, new LongSchemaVersion(0));
    }

    @Test
    public void testGetOrCreateSchema() throws Exception {
        admin.topics().createNonPartitionedTopic("persistent://my-property/my-ns/my-topic1");
        TestStreamObserver<CommandGetOrCreateSchemaResponse> response = TestStreamObserver.create();
        CommandGetOrCreateSchema getOrCreateSchema = Commands.newGetOrCreateSchema(
                "persistent://my-property/my-ns/my-topic1",
                org.apache.pulsar.client.api.Schema.STRING.getSchemaInfo());

        stub.getOrCreateSchema(getOrCreateSchema, response);

        SchemaVersion schemaVersion = pulsar.getSchemaRegistryService()
                .versionFromBytes(response.takeOneMessage().getSchemaVersion().toByteArray());
        assertEquals(schemaVersion, new LongSchemaVersion(0));
    }

    private static String getPayload(CommandMessage message) {
        String receivedMessage = new String(message.getMetadataAndPayload().getPayload().toByteArray());
        log.info("Received message: [{}]", receivedMessage);
        return receivedMessage;
    }

    private static class TestStreamObserver<T> implements StreamObserver<T> {

        public static final int TIMEOUT = 10000;

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

}
