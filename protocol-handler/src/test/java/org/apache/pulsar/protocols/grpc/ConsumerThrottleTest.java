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
package org.apache.pulsar.protocols.grpc;

import io.grpc.ManagedChannel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.protocols.grpc.api.CommandSubscribe;
import org.apache.pulsar.protocols.grpc.api.ConsumeInput;
import org.apache.pulsar.protocols.grpc.api.ConsumeOutput;
import org.apache.pulsar.protocols.grpc.api.PayloadType;
import org.apache.pulsar.protocols.grpc.api.PulsarGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.mockito.Mockito.doReturn;
import static org.testng.Assert.assertTrue;

/**
 * Tests for consumer throttling.
 */
public class ConsumerThrottleTest extends ProducerConsumerBase {

    private static final Logger log = LoggerFactory.getLogger(ConsumerThrottleTest.class);

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

        NettyChannelBuilder channelBuilder = NettyChannelBuilder
                .forAddress("localhost", grpcService.getListenPort().orElse(-1))
                .flowControlWindow(100)
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
    public void testGrpcConsumerFlowControl() throws Exception {
        log.info("-- Starting {} test --", methodName);

        final String topicName = "persistent://my-property/my-ns/my-topic1";

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer()
                .enableBatching(false)
                .topic(topicName);

        Producer<byte[]> producer = producerBuilder.create();
        for (int i = 0; i < 2000; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        // Subscribe
        CommandSubscribe subscribe = Commands.newSubscribe(topicName,
                "my-subscriber-name", CommandSubscribe.SubType.Exclusive, 0,
                "test", 0, PayloadType.BINARY);
        PulsarGrpc.PulsarStub consumerStub = Commands.attachConsumerParams(stub, subscribe);

        final CountDownLatch done = new CountDownLatch(1);
        final CountDownLatch consumerReady = new CountDownLatch(1);
        ClientResponseObserver<ConsumeInput, ConsumeOutput> consumeOutput =
                new ClientResponseObserver<ConsumeInput, ConsumeOutput>() {

                    ClientCallStreamObserver<ConsumeInput> requestStream;

                    @Override
                    public void beforeStart(final ClientCallStreamObserver<ConsumeInput> requestStream) {
                        this.requestStream = requestStream;
                        requestStream.disableAutoInboundFlowControl();
                    }

                    @Override
                    public void onNext(ConsumeOutput value) {
                        if (value.hasSubscribeSuccess()) {
                            consumerReady.countDown();
                        }
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        requestStream.request(1);
                    }

                    @Override
                    public void onError(Throwable t) {
                        t.printStackTrace();
                        done.countDown();
                    }

                    @Override
                    public void onCompleted() {
                        done.countDown();
                    }
                };
        StreamObserver<ConsumeInput> consumeInput = consumerStub.consume(consumeOutput);
        consumerReady.await();
        Thread.sleep(2000);

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topicName).get().get();
        org.apache.pulsar.broker.service.Consumer consumer = topic.getSubscription("my-subscriber-name")
                .getConsumers().get(0);
        consumer.updateRates();
        for (int i = 0; i < 3; i++) {
            Thread.sleep(1000);
            consumer.updateRates();
            assertTrue(consumer.getStats().msgRateOut < 100);
            assertTrue(consumer.getStats().msgRateOut > 80);
        }

        consumeInput.onCompleted();
        done.await();
        log.info("-- Exiting {} test --", methodName);
    }

}
