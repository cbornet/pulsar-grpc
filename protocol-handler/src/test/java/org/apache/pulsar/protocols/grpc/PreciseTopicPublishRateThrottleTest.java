package org.apache.pulsar.protocols.grpc;

import io.grpc.ManagedChannel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.pulsar.broker.service.AbstractTopic;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.protocols.grpc.api.CommandProducer;
import org.apache.pulsar.protocols.grpc.api.CommandSend;
import org.apache.pulsar.protocols.grpc.api.MessageIdData;
import org.apache.pulsar.protocols.grpc.api.PulsarGrpc;
import org.apache.pulsar.protocols.grpc.api.SendResult;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.mockito.Mockito.doReturn;
import static org.testng.Assert.assertTrue;

public class PreciseTopicPublishRateThrottleTest extends BrokerTestBase {

    private GrpcService grpcService;
    private PulsarGrpc.PulsarStub stub;
    private ManagedChannel channel;

    @Override
    protected void setup() throws Exception {
        super.baseSetup();

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

    @Override
    protected void cleanup() throws Exception {
        grpcService.close();
    }

    @Test
    public void testPreciseTopicPublishRateLimitingDisabled() throws Exception {
        PublishRate publishRate = new PublishRate(1,10);
        // disable precis topic publish rate limiting
        conf.setPreciseTopicPublishRateLimiterEnable(false);
        conf.setMaxPendingPublishRequestsPerConnection(0);
        setup();
        final String topic = "persistent://prop/ns-abc/testPrecisTopicPublishRateLimiting";

        // Lookup
        PulsarGrpc.PulsarBlockingStub blockingStub = PulsarGrpc.newBlockingStub(channel);
        blockingStub.lookupTopic(Commands.newLookup(topic, false));

        CommandProducer producer = Commands.newProducer(topic,
                "test", Collections.emptyMap());

        PulsarGrpc.PulsarStub producerStub = Commands.attachProducerParams(stub, producer);
        TestStreamObserver<SendResult> sendResult = TestStreamObserver.create();
        StreamObserver<CommandSend> commandSend = producerStub.produce(sendResult);

        assertTrue(sendResult.takeOneMessage(10000).hasProducerSuccess());

        Policies policies = new Policies();
        policies.publishMaxMessageRate = new HashMap<>();
        policies.publishMaxMessageRate.put("test", publishRate);

        Topic topicRef = pulsar.getBrokerService().getTopicReference(topic).get();
        Assert.assertNotNull(topicRef);
        ((AbstractTopic)topicRef).updateMaxPublishRate(policies);
        MessageIdData messageId = null;
        try {
            // first will be success
            messageId = producerSend(1, commandSend, sendResult, new byte[10], 500);
            Assert.assertNotNull(messageId);
            // second will be success
            producerSend(2, commandSend, sendResult, new byte[10], 500);
            Assert.assertNotNull(messageId);
        } catch (TimeoutException e) {
            // No-op
        }
        Thread.sleep(1000);
        try {
            messageId = producerSend(3, commandSend, sendResult, new byte[10], 1000);
        } catch (TimeoutException e) {
            // No-op
        }
        Assert.assertNotNull(messageId);
        commandSend.onCompleted();
        sendResult.waitForCompletion();

        super.internalCleanup();
    }

    @Test
    public void testProducerBlockedByPreciseTopicPublishRateLimiting() throws Exception {
        PublishRate publishRate = new PublishRate(1,10);
        conf.setPreciseTopicPublishRateLimiterEnable(true);
        conf.setMaxPendingPublishRequestsPerConnection(0);
        setup();
        final String topic = "persistent://prop/ns-abc/testPrecisTopicPublishRateLimiting";

        // Lookup
        PulsarGrpc.PulsarBlockingStub blockingStub = PulsarGrpc.newBlockingStub(channel);
        blockingStub.lookupTopic(Commands.newLookup(topic, false));

        CommandProducer producer = Commands.newProducer(topic,
                "test", Collections.emptyMap());

        PulsarGrpc.PulsarStub producerStub = Commands.attachProducerParams(stub, producer);
        TestStreamObserver<SendResult> sendResult = TestStreamObserver.create();
        StreamObserver<CommandSend> commandSend = producerStub.produce(sendResult);

        assertTrue(sendResult.takeOneMessage(10000).hasProducerSuccess());

        Policies policies = new Policies();
        policies.publishMaxMessageRate = new HashMap<>();
        policies.publishMaxMessageRate.put("test", publishRate);

        Topic topicRef = pulsar.getBrokerService().getTopicReference(topic).get();
        Assert.assertNotNull(topicRef);
        ((AbstractTopic)topicRef).updateMaxPublishRate(policies);
        MessageIdData messageId;
        try {
            // first will be success, and will set auto read to false
            messageId = producerSend(1, commandSend, sendResult, new byte[10], 500);
            Assert.assertNotNull(messageId);
            // second will be blocked
            producerSend(2, commandSend, sendResult, new byte[10], 500);
            Assert.fail("should failed, because producer blocked by topic publish rate limiting");
        } catch (TimeoutException e) {
            // No-op
        }
        super.internalCleanup();
    }

    @Test
    public void testPreciseTopicPublishRateLimitingProduceRefresh() throws Exception {
        PublishRate publishRate = new PublishRate(1,10);
        conf.setPreciseTopicPublishRateLimiterEnable(true);
        conf.setMaxPendingPublishRequestsPerConnection(0);
        setup();
        final String topic = "persistent://prop/ns-abc/testPrecisTopicPublishRateLimiting";

        // Lookup
        PulsarGrpc.PulsarBlockingStub blockingStub = PulsarGrpc.newBlockingStub(channel);
        blockingStub.lookupTopic(Commands.newLookup(topic, false));

        CommandProducer producer = Commands.newProducer(topic,
                "test", Collections.emptyMap());

        PulsarGrpc.PulsarStub producerStub = Commands.attachProducerParams(stub, producer);
        TestStreamObserver<SendResult> sendResult = TestStreamObserver.create();
        StreamObserver<CommandSend> commandSend = producerStub.produce(sendResult);

        assertTrue(sendResult.takeOneMessage(10000).hasProducerSuccess());

        Policies policies = new Policies();
        policies.publishMaxMessageRate = new HashMap<>();
        policies.publishMaxMessageRate.put("test", publishRate);

        Topic topicRef = pulsar.getBrokerService().getTopicReference(topic).get();
        Assert.assertNotNull(topicRef);
        ((AbstractTopic)topicRef).updateMaxPublishRate(policies);
        MessageIdData messageId = null;
        try {
            // first will be success, and will set auto read to false
            messageId = producerSend(1, commandSend, sendResult, new byte[10], 500);
            Assert.assertNotNull(messageId);
            // second will be blocked
            producerSend(1, commandSend, sendResult, new byte[10], 500);
            Assert.fail("should failed, because producer blocked by topic publish rate limiting");
        } catch (TimeoutException e) {
            // No-op
        }
        Thread.sleep(1000);
        try {
            messageId = producerSend(1, commandSend, sendResult, new byte[10], 1000);
        } catch (TimeoutException e) {
            // No-op
        }
        Assert.assertNotNull(messageId);
        super.internalCleanup();
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

        public T takeOneMessage(int timeoutMs) throws InterruptedException, TimeoutException {
            T poll = queue.poll(timeoutMs, TimeUnit.MILLISECONDS);
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

    private MessageIdData producerSend(int i, StreamObserver<CommandSend> commandSend, TestStreamObserver<SendResult> sendResult,
            byte[] bytes, int timeoutMs) throws InterruptedException, TimeoutException {
        PulsarApi.MessageMetadata messageMetadata = PulsarApi.MessageMetadata.newBuilder()
                .setPublishTime(System.currentTimeMillis())
                .setProducerName("prod-name")
                .setSequenceId(i)
                .build();
        ByteBuf data = Unpooled.wrappedBuffer(bytes);
        commandSend.onNext(Commands.newSend(i, 1, messageMetadata, data));
        return sendResult.takeOneMessage(timeoutMs).getSendReceipt().getMessageId();
    }
}
