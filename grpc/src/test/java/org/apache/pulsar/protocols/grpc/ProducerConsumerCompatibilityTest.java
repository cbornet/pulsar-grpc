package org.apache.pulsar.protocols.grpc;

import com.google.common.collect.Sets;
import io.grpc.ManagedChannel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.protocol.Commands.ChecksumType;
import org.apache.pulsar.protocols.grpc.api.CommandAck.AckType;
import org.apache.pulsar.protocols.grpc.api.CommandMessage;
import org.apache.pulsar.protocols.grpc.api.CommandProducer;
import org.apache.pulsar.protocols.grpc.api.CommandSend;
import org.apache.pulsar.protocols.grpc.api.CommandSubscribe;
import org.apache.pulsar.protocols.grpc.api.ConsumeInput;
import org.apache.pulsar.protocols.grpc.api.ConsumeOutput;
import org.apache.pulsar.protocols.grpc.api.MessageIdData;
import org.apache.pulsar.protocols.grpc.api.PulsarGrpc;
import org.apache.pulsar.protocols.grpc.api.SendResult;
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

import static org.apache.pulsar.common.protocol.Commands.parseMessageMetadata;
import static org.mockito.Mockito.doReturn;
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

    @Test(timeOut = 30000)
    public void testPulsarProducerAndGrpcConsumer() throws Exception {
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
        Set<String> messageSet = Sets.newHashSet();
        for (int i = 0; i < 10; i++) {
            message = consumeOutput.takeOneMessage().getMessage();
            ByteBuf headersAndPayload = Unpooled.wrappedBuffer(message.getHeadersAndPayload().toByteArray());
            parseMessageMetadata(headersAndPayload);
            ByteBuf payload = Unpooled.copiedBuffer(headersAndPayload);
            String receivedMessage = new String(payload.array());
            log.debug("Received message: [{}]", receivedMessage);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }
        MessageIdData messageId = message.getMessageId();
        // Acknowledge the consumption of all messages at once
        consumeInput.onNext(Commands.newAck(messageId.getLedgerId(), messageId.getEntryId(),
                AckType.Cumulative, null, Collections.emptyMap()));
        Thread.sleep(100);
        consumeInput.onCompleted();
        consumeOutput.waitForCompletion();
        log.info("-- Exiting {} test --", methodName);
    }

    @Test(timeOut = 30000)
    public void testSyncProducerAndConsumer() throws Exception {
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
            commandSend.onNext(Commands.newSend(i, 1, ChecksumType.Crc32c, messageMetadata, data));
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

    private static class TestStreamObserver<T> implements StreamObserver<T> {

        public static <T> TestStreamObserver<T> create() {
            return new TestStreamObserver<>();
        }

        private final LinkedBlockingQueue<T> queue = new LinkedBlockingQueue<>();
        private final CompletableFuture<Throwable> error = new CompletableFuture<>();
        private final CountDownLatch complete = new CountDownLatch(1);
        private final CountDownLatch errorOrComplete = new CountDownLatch(1);

        private TestStreamObserver() {
        }

        @Override
        public void onNext(T value) {
            queue.add(value);
        }

        @Override
        public void onError(Throwable t) {
            errorOrComplete.countDown();
            error.complete(t);
        }

        @Override
        public void onCompleted() {
            errorOrComplete.countDown();
            complete.countDown();
        }

        public T takeOneMessage() throws InterruptedException {
            return queue.take();
        }

        public T pollOneMessage() throws InterruptedException {
            return queue.poll();
        }

        public Throwable waitForError() throws ExecutionException, InterruptedException {
            return error.get();
        }

        public void waitForCompletion() throws InterruptedException {
            complete.await();
        }

        public void waitForErrorOrCompletion() throws InterruptedException {
            errorOrComplete.await();
        }
    }

}
