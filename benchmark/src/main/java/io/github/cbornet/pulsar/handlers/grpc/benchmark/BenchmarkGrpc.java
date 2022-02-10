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
package io.github.cbornet.pulsar.handlers.grpc.benchmark;

import com.google.common.collect.Sets;
import io.github.cbornet.pulsar.handlers.grpc.Commands;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandFlow;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandProducer;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandSend;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandSubscribe;
import io.github.cbornet.pulsar.handlers.grpc.api.ConsumeInput;
import io.github.cbornet.pulsar.handlers.grpc.api.ConsumeOutput;
import io.github.cbornet.pulsar.handlers.grpc.api.PulsarGrpc;
import io.github.cbornet.pulsar.handlers.grpc.api.SendResult;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;

import java.util.Collections;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * Benchmarking utility program.
 * Creates one producer and one consumer on a random topic and tries to max out the throughput of messages.
 */
public class BenchmarkGrpc {

    public static final String PULSAR_SERVICE_URL = "http://localhost:8080";
    public static final String GRPC_SERVICE_HOST = "localhost";
    public static final int GRPC_SERVICE_PORT = 9080;
    public static final String TENANT = "grpc";
    public static final String CLUSTER = "standalone";
    public static final String NAMESPACE_PREFIX = "grpc/benchmark";
    public static final int BYTES_IN_ONE_MEGABYTE = 1024 * 1024;

    public static final int DEFAULT_MESSAGE_SIZE = 1024;
    public static final int PENDING_MESSAGE_BUFFER_SIZE = 10_000;
    public static final int ENSEMBLE_SIZE = 1;
    public static final int WRITE_QUORUM = 1;
    public static final int ACK_QUORUM = 1;
    public static final boolean DEDUPLICATION_ENABLED = false;

    public static void main(String[] args) throws InterruptedException, PulsarClientException, PulsarAdminException {

        int messageSize = DEFAULT_MESSAGE_SIZE;
        if (args.length > 0) {
            messageSize = Integer.parseInt(args[0]);
        }

        System.out.println("Starting benchmark with message size: " + messageSize);

        CountDownLatch producerReady = new CountDownLatch(1);
        CountDownLatch consumerReady = new CountDownLatch(1);
        long sends = 0;
        LongAdder sendErrors = new LongAdder();
        LongAdder sendAcks = new LongAdder();
        LongAdder receivedMsg = new LongAdder();

        PulsarAdminBuilder pulsarAdminBuilder = PulsarAdmin.builder().serviceHttpUrl(PULSAR_SERVICE_URL);

        PulsarAdmin adminClient = pulsarAdminBuilder.build();

        // Create namespace and set the configuration
        if (!adminClient.tenants().getTenants().contains(TENANT)) {
            try {
                adminClient.tenants().createTenant(TENANT,
                    TenantInfo.builder()
                        .allowedClusters(Sets.newHashSet(CLUSTER))
                        .build());
            } catch (PulsarAdminException.ConflictException e) {
                // Ignore. This can happen when multiple workers are initializing at the same time
            }
        }

        String namespace = NAMESPACE_PREFIX + "-" + UUID.randomUUID();
        adminClient.namespaces().createNamespace(namespace);
        System.out.println("Created Pulsar namespace " + namespace);

        adminClient.namespaces().setPersistence(namespace,
                new PersistencePolicies(ENSEMBLE_SIZE, WRITE_QUORUM, ACK_QUORUM, 1.0));

        adminClient.namespaces().setBacklogQuota(namespace,
            BacklogQuota.builder()
                .limitSize(-1L)
                .limitTime(-1)
                .retentionPolicy(BacklogQuota.RetentionPolicy.producer_exception)
                .build());
        adminClient.namespaces().setDeduplicationStatus(namespace, DEDUPLICATION_ENABLED);

        String topic = "persistent://" + namespace + "/test-" + UUID.randomUUID();
        System.out.println("Topic: " + topic);

        NettyChannelBuilder channelBuilder = NettyChannelBuilder
                .forAddress(GRPC_SERVICE_HOST, GRPC_SERVICE_PORT)
                .usePlaintext()
                .directExecutor()
                .negotiationType(NegotiationType.PLAINTEXT);
        ManagedChannel channel = channelBuilder.build();

        PulsarGrpc.PulsarStub stub = PulsarGrpc.newStub(channel);

        PulsarGrpc.PulsarBlockingStub blockingStub = PulsarGrpc.newBlockingStub(channel);

        try {
            blockingStub.lookupTopic(Commands.newLookup(topic, false));
        } catch (StatusRuntimeException e) {
            // OK
        }

        CommandSubscribe subscribe = Commands.newSubscribe(topic,
                "benchmark-" + UUID.randomUUID(), CommandSubscribe.SubType.Exclusive, 0,
                "test", true /* isDurable */, null /* startMessageId */, Collections.emptyMap(), false,
                false /* isReplicated */, CommandSubscribe.InitialPosition.Latest, 0, null,
                true /* createTopicIfDoesNotExist */, null);
        PulsarGrpc.PulsarStub consumerStub = Commands.attachConsumerParams(stub, subscribe);

        StreamObserver<ConsumeOutput> consumeOutput = new StreamObserver<ConsumeOutput>() {
            @Override
            public void onNext(ConsumeOutput consumeOutput) {
                if (consumeOutput.hasMessage()) {
                    receivedMsg.increment();
                } else if (consumeOutput.hasSubscribeSuccess()) {
                    consumerReady.countDown();
                    System.out.println("Created consumer");
                }
            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onCompleted() {

            }
        };
        StreamObserver<ConsumeInput> consumeInput = consumerStub.consume(consumeOutput);
        if (!consumerReady.await(5, TimeUnit.SECONDS)) {
            throw new RuntimeException("Consumer creation timeout");
        }
        consumeInput.onNext(
                ConsumeInput.newBuilder()
                        .setFlow(
                                CommandFlow.newBuilder().setMessagePermits(1_000_000).build())
                        .build());

        StreamObserver<SendResult> sendResults = new StreamObserver<SendResult>() {

            @Override
            public void onNext(SendResult sendResult) {
                if (sendResult.hasProducerSuccess()) {
                    producerReady.countDown();
                    System.out.println("Created producer");
                } else {
                    sendAcks.increment();
                    if (sendResult.hasSendError()) {
                        sendErrors.increment();
                    }
                }
            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onCompleted() {

            }
        };

        CommandProducer grpcProducer = Commands.newProducer(
                topic, "benchmark-" + UUID.randomUUID(), Collections.emptyMap());

        PulsarGrpc.PulsarStub producer = Commands.attachProducerParams(stub, grpcProducer);
        StreamObserver<CommandSend> produce = producer.produce(sendResults);

        if (!producerReady.await(5, TimeUnit.SECONDS)) {
            throw new RuntimeException("Producer creation timeout");
        }

        long previousAcks = 0;
        long previousReceivedMsg = 0;
        long previousTime = System.currentTimeMillis();


        byte[] dataBytes = new byte[messageSize];
        Random random = new Random();
        random.nextBytes(dataBytes);
        ByteBuf data = Unpooled.wrappedBuffer(dataBytes);

        while (true) {
            long acks = sendAcks.sum();
            long totalMsg = receivedMsg.sum();
            if (sends - acks < PENDING_MESSAGE_BUFFER_SIZE) {
                MessageMetadata messageMetadata =
                        new MessageMetadata()
                                .setPublishTime(System.currentTimeMillis())
                                .setProducerName("producer-name")
                                .setSequenceId(sends);

                CommandSend commandSend = Commands.newSend(sends, 1, messageMetadata, data);
                data.resetReaderIndex();
                produce.onNext(commandSend);
                sends++;
            }
            long now = System.currentTimeMillis();
            long timedelta = now - previousTime;
            if (timedelta > 10_000) {
                System.out.println("Producer speed: "
                        + (acks - previousAcks) * 1000f / timedelta + " msg/s - "
                        + (acks - previousAcks) * 1000f * messageSize / (timedelta * BYTES_IN_ONE_MEGABYTE) + " MBps");
                System.out.println("Consumer speed: "
                        + (totalMsg - previousReceivedMsg) * 1000f / timedelta + " msg/s - "
                        + (totalMsg - previousReceivedMsg) * 1000f * messageSize / (timedelta * BYTES_IN_ONE_MEGABYTE)
                        + " MBps");
                previousTime = now;
                previousAcks = acks;
                previousReceivedMsg = totalMsg;
            }
        }

    }
}
