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

import com.google.common.collect.Maps;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandAck.AckType;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandAckResponse;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandConsumerStatsResponse;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandEndTxnOnPartitionResponse;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandEndTxnOnSubscriptionResponse;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandEndTxnResponse;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandError;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandGetTopicsOfNamespace;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandGetTopicsOfNamespaceResponse;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandNewTxn;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandNewTxnResponse;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandProduceSingle;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandProducer;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandSend;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandSendReceipt;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandSubscribe;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandSubscribe.SubType;
import io.github.cbornet.pulsar.handlers.grpc.api.ConsumeInput;
import io.github.cbornet.pulsar.handlers.grpc.api.ConsumeOutput;
import io.github.cbornet.pulsar.handlers.grpc.api.PulsarGrpc;
import io.github.cbornet.pulsar.handlers.grpc.api.SendResult;
import io.github.cbornet.pulsar.handlers.grpc.api.ServerError;
import io.github.cbornet.pulsar.handlers.grpc.api.TxnAction;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.TransactionMetadataStoreService;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.authorization.PulsarAuthorizationProvider;
import org.apache.pulsar.broker.cache.ConfigurationCacheService;
import org.apache.pulsar.broker.cache.LocalZooKeeperCacheService;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentSubscription;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentTopic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.service.schema.DefaultSchemaRegistryService;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.impl.BacklogQuotaImpl;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.schema.LongSchemaVersion;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.metadata.impl.LocalMemoryMetadataStore;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TxnMeta;
import org.apache.pulsar.transaction.coordinator.impl.InMemTransactionMetadataStoreProvider;
import org.apache.pulsar.transaction.coordinator.proto.TxnStatus;
import org.apache.pulsar.zookeeper.ZooKeeperCache;
import org.apache.pulsar.zookeeper.ZooKeeperDataCache;
import org.apache.zookeeper.ZooKeeper;
import org.awaitility.Awaitility;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static io.github.cbornet.pulsar.handlers.grpc.Constants.CONSUMER_PARAMS_METADATA_KEY;
import static io.github.cbornet.pulsar.handlers.grpc.Constants.ERROR_CODE_METADATA_KEY;
import static org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest.createMockBookKeeper;
import static org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest.createMockZooKeeper;
import static org.apache.pulsar.broker.cache.ConfigurationCacheService.POLICIES;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.matches;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * Tests for {@link PulsarGrpcService}.
 */
public class PulsarGrpcServiceTest {

    private ServiceConfiguration svcConfig;
    protected BrokerService brokerService;
    private ManagedLedgerFactory mlFactoryMock;
    private PulsarService pulsar;
    private ConfigurationCacheService configCacheService;
    protected NamespaceService namespaceService;
    private TransactionMetadataStoreService transactionMetadataStoreService;

    protected final String successTopicName = "persistent://prop/use/ns-abc/successTopic";
    private final String failTopicName = "persistent://prop/use/ns-abc/failTopic";
    private final String nonOwnedTopicName = "persistent://prop/use/ns-abc/success-not-owned-topic";
    private final String encryptionRequiredTopicName = "persistent://prop/use/ns-abc/successEncryptionRequiredTopic";
    private final String successSubName = "successSub";
    private final String nonExistentTopicName =
            "persistent://nonexistent-prop/nonexistent-cluster/nonexistent-namespace/successNonExistentTopic";
    private final String topicWithNonLocalCluster = "persistent://prop/usw/ns-abc/successTopic";

    private ManagedLedger ledgerMock;
    private ManagedCursor cursorMock;
    private OrderedExecutor executor;
    private EventLoopGroup eventLoopGroup;

    private Server server;
    private ManagedChannel channel;
    private PulsarGrpc.PulsarStub stub;
    private PulsarGrpc.PulsarBlockingStub blockingStub;

    @BeforeMethod
    public void setup() throws Exception {
        ledgerMock = mock(ManagedLedger.class);
        cursorMock = mock(ManagedCursor.class);
        eventLoopGroup = new NioEventLoopGroup();
        executor = OrderedExecutor.newBuilder().numThreads(1).build();
        svcConfig = spy(new ServiceConfiguration());
        svcConfig.setBrokerShutdownTimeoutMs(0L);
        pulsar = spy(new PulsarService(svcConfig));
        doReturn(new DefaultSchemaRegistryService()).when(pulsar).getSchemaRegistryService();

        svcConfig.setKeepAliveIntervalSeconds(inSec(1, TimeUnit.SECONDS));
        svcConfig.setBacklogQuotaCheckEnabled(false);
        doReturn(svcConfig).when(pulsar).getConfiguration();

        doReturn("use").when(svcConfig).getClusterName();

        mlFactoryMock = mock(ManagedLedgerFactory.class);
        doReturn(mlFactoryMock).when(pulsar).getManagedLedgerFactory();
        ZooKeeperCache cache = mock(ZooKeeperCache.class);
        doReturn(30).when(cache).getZkOperationTimeoutSeconds();
        doReturn(cache).when(pulsar).getLocalZkCache();

        ZooKeeper mockZk = createMockZooKeeper();
        doReturn(mockZk).when(pulsar).getZkClient();
        doReturn(createMockBookKeeper(executor))
                .when(pulsar).getBookKeeperClient();

        configCacheService = mock(ConfigurationCacheService.class);
        ZooKeeperDataCache<Policies> zkDataCache = mock(ZooKeeperDataCache.class);
        doReturn(Optional.empty()).when(zkDataCache).get(any());
        doReturn(zkDataCache).when(configCacheService).policiesCache();
        doReturn(configCacheService).when(pulsar).getConfigurationCache();

        LocalZooKeeperCacheService zkCache = mock(LocalZooKeeperCacheService.class);
        doReturn(CompletableFuture.completedFuture(Optional.empty())).when(zkDataCache).getAsync(any());
        doReturn(zkDataCache).when(zkCache).policiesCache();
        doReturn(configCacheService).when(pulsar).getConfigurationCache();
        doReturn(zkCache).when(pulsar).getLocalZkCacheService();

        brokerService = spy(new BrokerService(pulsar, eventLoopGroup));
        BrokerInterceptor interceptor = mock(BrokerInterceptor.class);
        doReturn(interceptor).when(brokerService).getInterceptor();
        doReturn(brokerService).when(pulsar).getBrokerService();
        doReturn(executor).when(pulsar).getOrderedExecutor();

        namespaceService = mock(NamespaceService.class);
        doReturn(CompletableFuture.completedFuture(null)).when(namespaceService).getBundleAsync(any());
        doReturn(namespaceService).when(pulsar).getNamespaceService();
        doReturn(true).when(namespaceService).isServiceUnitOwned(any());
        doReturn(true).when(namespaceService).isServiceUnitActive(any());
        doReturn(CompletableFuture.completedFuture(true)).when(namespaceService).checkTopicOwnership(any());

        doReturn(new LocalMemoryMetadataStore(null, null)).when(pulsar).getLocalMetadataStore();

        transactionMetadataStoreService = new TransactionMetadataStoreService(
            new InMemTransactionMetadataStoreProvider(), pulsar, null, null);
        doReturn(transactionMetadataStoreService).when(pulsar).getTransactionMetadataStoreService();

        setupMLAsyncCallbackMocks();

        String serverName = InProcessServerBuilder.generateName();

        server = InProcessServerBuilder.forName(serverName)
                .addService(ServerInterceptors.intercept(
                        new PulsarGrpcService(brokerService, svcConfig, new NioEventLoopGroup()),
                        Collections.singletonList(new GrpcServerInterceptor())
                ))
                .build();

        server.start();

        channel = InProcessChannelBuilder.forName(serverName).build();
        stub = PulsarGrpc.newStub(channel);
        blockingStub = PulsarGrpc.newBlockingStub(channel);
    }

    @AfterMethod
    public void teardown() throws Exception {
        channel.shutdown();
        channel.awaitTermination(30, TimeUnit.SECONDS);
        server.shutdown();
        server.awaitTermination(30, TimeUnit.SECONDS);
        pulsar.close();
        brokerService.close();
        executor.shutdownNow();
        eventLoopGroup.shutdownGracefully().get();
    }

    private int inSec(int time, TimeUnit unit) {
        return (int) TimeUnit.SECONDS.convert(time, unit);
    }

    @Test
    public void testProduce() throws Exception {
        // test PRODUCER success case
        CommandProducer producerParams = Commands.newProducer(successTopicName, "prod-name", Collections.emptyMap());
        PulsarGrpc.PulsarStub producerStub = Commands.attachProducerParams(stub, producerParams);

        TestStreamObserver<SendResult> observer = TestStreamObserver.create();
        StreamObserver<CommandSend> request = producerStub.produce(observer);

        assertTrue(observer.takeOneMessage().hasProducerSuccess());

        PersistentTopic topicRef = (PersistentTopic) brokerService.getTopicReference(successTopicName).get();

        assertNotNull(topicRef);
        assertEquals(topicRef.getProducers().size(), 1);

        // test PRODUCER error case
        producerParams = Commands.newProducer(failTopicName, "prod-name-2", Collections.emptyMap());
        producerStub = Commands.attachProducerParams(stub, producerParams);

        TestStreamObserver<SendResult> observer2 = TestStreamObserver.create();
        producerStub.produce(observer2);
        assertErrorIsStatusExceptionWithServerError(observer2.waitForError(), Status.FAILED_PRECONDITION,
                ServerError.PersistenceError);
        assertFalse(pulsar.getBrokerService().getTopicReference(failTopicName).isPresent());

        request.onCompleted();
        observer.waitForCompletion();
        assertEquals(topicRef.getProducers().size(), 0);
    }

    @Test
    public void testProduceMissingHeader() throws Exception {
        TestStreamObserver<SendResult> observer = TestStreamObserver.create();
        stub.produce(observer);

        Status actualStatus = Status.fromThrowable(observer.waitForError());
        assertEquals(actualStatus.getCode(), Status.Code.INVALID_ARGUMENT);
    }

    @Test
    public void testProducerOnNotOwnedTopic() throws Exception {
        // Force the case where the broker doesn't own any topic
        doReturn(false).when(namespaceService).isServiceUnitActive(any(TopicName.class));

        // test PRODUCER failure case
        CommandProducer producerParams = Commands.newProducer(nonOwnedTopicName, "prod-name", Collections.emptyMap());
        verifyProduceFails(producerParams, Status.FAILED_PRECONDITION, ServerError.ServiceNotReady);

        assertFalse(pulsar.getBrokerService().getTopicReference(nonOwnedTopicName).isPresent());
    }

    @Test
    public void testProducerCommandWithAuthorizationPositive() throws Exception {
        AuthorizationService authorizationService = mock(AuthorizationService.class);
        doReturn(CompletableFuture.completedFuture(true)).when(authorizationService)
                .allowTopicOperationAsync(Mockito.any(),
                        Mockito.any(), Mockito.any(), Mockito.any());
        doReturn(authorizationService).when(brokerService).getAuthorizationService();
        doReturn(true).when(brokerService).isAuthenticationEnabled();
        doReturn(true).when(brokerService).isAuthorizationEnabled();

        // test PRODUCER success case
        CommandProducer producerParams = Commands.newProducer(successTopicName, "prod-name", Collections.emptyMap());
        PulsarGrpc.PulsarStub producerStub = Commands.attachProducerParams(stub, producerParams);

        TestStreamObserver<SendResult> observer = TestStreamObserver.create();
        StreamObserver<CommandSend> request = producerStub.produce(observer);

        assertTrue(observer.takeOneMessage().hasProducerSuccess());

        PersistentTopic topicRef = (PersistentTopic) brokerService.getTopicReference(successTopicName).get();

        assertNotNull(topicRef);
        assertEquals(topicRef.getProducers().size(), 1);

        request.onCompleted();
        observer.waitForCompletion();
        assertEquals(topicRef.getProducers().size(), 0);
    }

    @Test
    public void testNonExistentTopic() throws Exception {
        ZooKeeperDataCache<Policies> zkDataCache = mock(ZooKeeperDataCache.class);
        ConfigurationCacheService configCacheService = mock(ConfigurationCacheService.class);
        doReturn(configCacheService).when(pulsar).getConfigurationCache();
        doReturn(zkDataCache).when(configCacheService).policiesCache();
        doReturn(CompletableFuture.completedFuture(Optional.empty())).when(zkDataCache)
                .getAsync(matches(".*nonexistent.*"));

        AuthorizationService authorizationService = spy(new AuthorizationService(svcConfig, configCacheService));
        doReturn(authorizationService).when(brokerService).getAuthorizationService();
        doReturn(true).when(brokerService).isAuthorizationEnabled();
        svcConfig.setAuthorizationEnabled(true);
        Field providerField = AuthorizationService.class.getDeclaredField("provider");
        providerField.setAccessible(true);
        PulsarAuthorizationProvider authorizationProvider =
                spy(new PulsarAuthorizationProvider(svcConfig, configCacheService));
        providerField.set(authorizationService, authorizationProvider);
        doReturn(CompletableFuture.completedFuture(false)).when(authorizationProvider)
                .isSuperUser(Mockito.anyString(), Mockito.any(), Mockito.any());

        // Test producer creation
        CommandProducer producerParams =
                Commands.newProducer(nonExistentTopicName, "prod-name", Collections.emptyMap());
        verifyProduceFails(producerParams, Status.PERMISSION_DENIED, ServerError.AuthorizationError);

        // Test consumer creation
        CommandSubscribe consumerParams = Commands.newSubscribe(nonExistentTopicName,
                successSubName, SubType.Exclusive, 0, "test" /* consumer name */, 0);
        verifyConsumeFails(consumerParams, Status.PERMISSION_DENIED, ServerError.AuthorizationError);
    }

    @Test
    public void testClusterAccess() throws Exception {
        svcConfig.setAuthorizationEnabled(true);
        AuthorizationService authorizationService = spy(new AuthorizationService(svcConfig, configCacheService));
        Field providerField = AuthorizationService.class.getDeclaredField("provider");
        providerField.setAccessible(true);
        PulsarAuthorizationProvider authorizationProvider =
                spy(new PulsarAuthorizationProvider(svcConfig, configCacheService));
        providerField.set(authorizationService, authorizationProvider);
        doReturn(authorizationService).when(brokerService).getAuthorizationService();
        doReturn(true).when(brokerService).isAuthorizationEnabled();
        doReturn(CompletableFuture.completedFuture(false)).when(authorizationProvider)
                .isSuperUser(Mockito.anyString(), Mockito.any(), Mockito.any());
        doReturn(CompletableFuture.completedFuture(false)).when(authorizationProvider)
                .validateTenantAdminAccess(Mockito.anyString(), Mockito.any(), Mockito.any());
        doReturn(CompletableFuture.completedFuture(true)).when(authorizationProvider)
                .checkPermission(any(TopicName.class), Mockito.any(),
                        any(AuthAction.class));

        CommandProducer producerParams = Commands.newProducer(successTopicName, "prod-name", Collections.emptyMap());
        PulsarGrpc.PulsarStub producerStub = Commands.attachProducerParams(stub, producerParams);

        TestStreamObserver<SendResult> observer = TestStreamObserver.create();
        StreamObserver<CommandSend> request = producerStub.produce(observer);

        assertTrue(observer.takeOneMessage().hasProducerSuccess());

        request.onCompleted();
        observer.waitForCompletion();

        producerParams = Commands.newProducer(topicWithNonLocalCluster, "prod-name", Collections.emptyMap());
        verifyProduceFails(producerParams, Status.PERMISSION_DENIED, ServerError.AuthorizationError);
    }

    @Test
    public void testNonExistentTopicSuperUserAccess() throws Exception {
        AuthorizationService authorizationService = spy(new AuthorizationService(svcConfig, configCacheService));
        doReturn(authorizationService).when(brokerService).getAuthorizationService();
        doReturn(true).when(brokerService).isAuthorizationEnabled();
        Field providerField = AuthorizationService.class.getDeclaredField("provider");
        providerField.setAccessible(true);
        PulsarAuthorizationProvider authorizationProvider =
                spy(new PulsarAuthorizationProvider(svcConfig, configCacheService));
        providerField.set(authorizationService, authorizationProvider);
        doReturn(CompletableFuture.completedFuture(true)).when(authorizationProvider)
                .isSuperUser(Mockito.anyString(), Mockito.any(), Mockito.any());

        // Test producer creation
        CommandProducer producerParams =
                Commands.newProducer(nonExistentTopicName, "prod-name", Collections.emptyMap());
        PulsarGrpc.PulsarStub producerStub = Commands.attachProducerParams(stub, producerParams);
        TestStreamObserver<SendResult> observer = new TestStreamObserver<>();
        StreamObserver<CommandSend> request = producerStub.produce(observer);

        assertTrue(observer.takeOneMessage().hasProducerSuccess());

        PersistentTopic topicRef = (PersistentTopic) brokerService.getTopicReference(nonExistentTopicName).get();
        assertNotNull(topicRef);
        assertEquals(topicRef.getProducers().size(), 1);
        request.onCompleted();
        observer.waitForCompletion();

        // Test consumer creation
        Metadata headers = new Metadata();
        CommandSubscribe consumerParams = Commands.newSubscribe(nonExistentTopicName,
                successSubName, SubType.Exclusive, 0, "test" /* consumer name */, 0);

        headers.put(CONSUMER_PARAMS_METADATA_KEY, consumerParams.toByteArray());

        PulsarGrpc.PulsarStub consumerStub = MetadataUtils.attachHeaders(stub, headers);

        TestStreamObserver<ConsumeOutput> observer2 = new TestStreamObserver<>();

        StreamObserver<ConsumeInput> consumeInput = consumerStub.consume(observer2);

        assertTrue(observer2.takeOneMessage().hasSubscribeSuccess());

        topicRef = (PersistentTopic) brokerService.getTopicReference(nonExistentTopicName).get();
        assertNotNull(topicRef);
        assertTrue(topicRef.getSubscriptions().containsKey(successSubName));
        assertTrue(topicRef.getSubscription(successSubName).getDispatcher().isConsumerConnected());

        consumeInput.onCompleted();
        observer2.waitForCompletion();
    }

    @Test
    public void testProducerCommandWithAuthorizationNegative() throws Exception {
        AuthorizationService authorizationService = mock(AuthorizationService.class);
        doReturn(CompletableFuture.completedFuture(false)).when(authorizationService)
                .allowTopicOperationAsync(Mockito.any(),
                        Mockito.any(), Mockito.any(), Mockito.any());
        doReturn(authorizationService).when(brokerService).getAuthorizationService();
        doReturn(true).when(brokerService).isAuthenticationEnabled();
        doReturn(true).when(brokerService).isAuthorizationEnabled();
        doReturn("prod1").when(brokerService).generateUniqueProducerName();

        CommandProducer producerParams = Commands.newProducer(successTopicName, null, Collections.emptyMap());
        verifyProduceFails(producerParams, Status.PERMISSION_DENIED, ServerError.AuthorizationError);
    }

    @Test
    public void testSendCommand() throws Exception {
        CommandProducer producerParams = Commands.newProducer(successTopicName, "prod-name", Collections.emptyMap());
        PulsarGrpc.PulsarStub producerStub = Commands.attachProducerParams(stub, producerParams);

        TestStreamObserver<SendResult> observer = TestStreamObserver.create();
        StreamObserver<CommandSend> request = producerStub.produce(observer);

        assertTrue(observer.takeOneMessage().hasProducerSuccess());

        // test SEND success
        org.apache.pulsar.common.api.proto.MessageMetadata messageMetadata =
            new org.apache.pulsar.common.api.proto.MessageMetadata()
                .setPublishTime(System.currentTimeMillis())
                .setProducerName("prod-name")
                .setSequenceId(0);
        ByteBuf data = Unpooled.buffer(1024);

        CommandSend clientCommand = Commands.newSend(1, 0, 1, messageMetadata, data);

        request.onNext(clientCommand);
        SendResult sendReceipt = observer.takeOneMessage();
        assertTrue(sendReceipt.hasSendReceipt());

        request.onCompleted();
        observer.waitForCompletion();
    }

    @Test
    public void testUseSameProducerName() throws Exception {
        String producerName = "my-producer";
        // Create producer first time
        CommandProducer producerParams = Commands.newProducer(successTopicName, producerName, Collections.emptyMap());
        PulsarGrpc.PulsarStub producerStub = Commands.attachProducerParams(stub, producerParams);

        TestStreamObserver<SendResult> observer = TestStreamObserver.create();
        StreamObserver<CommandSend> produce = producerStub.produce(observer);

        assertTrue(observer.takeOneMessage().hasProducerSuccess());

        // Create producer second time
        verifyProduceFails(producerParams, Status.FAILED_PRECONDITION, ServerError.ProducerBusy);

        produce.onCompleted();
        observer.waitForCompletion();
    }

    @Test
    public void testCreateProducerTimeout() throws Exception {
        // Delay the topic creation in a deterministic way
        CompletableFuture<Runnable> openTopicFuture = new CompletableFuture<>();
        doAnswer(invocationOnMock -> {
            openTopicFuture.complete(() -> {
                ((AsyncCallbacks.OpenLedgerCallback) invocationOnMock.getArguments()[2])
                        .openLedgerComplete(ledgerMock, null);
            });
            return null;
        }).when(mlFactoryMock).asyncOpen(matches(".*success.*"), any(ManagedLedgerConfig.class),
                any(AsyncCallbacks.OpenLedgerCallback.class), any(Supplier.class), any());

        // In a create producer timeout from client side we expect to see this sequence of commands :
        // 1. create producer
        // 2. close producer (when the timeout is triggered, which may be before the producer was created on the broker
        // 3. create producer (triggered by reconnection logic)

        // These operations need to be serialized, to allow the last create producer to finally succeed
        // (There can be more create/close pairs in the sequence, depending on the client timeout

        String producerName = "my-producer";

        CommandProducer createProducer1 = Commands.newProducer(successTopicName, producerName, Collections.emptyMap());
        PulsarGrpc.PulsarStub produceStub = Commands.attachProducerParams(stub, createProducer1);

        TestStreamObserver<SendResult> observer = TestStreamObserver.create();
        StreamObserver<CommandSend> commandSend1 = produceStub.produce(observer);

        commandSend1.onCompleted();

        TestStreamObserver<SendResult> observer2 = TestStreamObserver.create();
        StreamObserver<CommandSend> commandSend2 = produceStub.produce(observer2);

        // Complete the topic opening: It will make 2nd producer creation successful
        openTopicFuture.get().run();

        // Close succeeds (due to race, it will either end with a complete or an error)
        observer.waitForErrorOrCompletion();

        // 2nd producer will be successfully created as topic is open by then
        assertTrue(observer2.takeOneMessage().hasProducerSuccess());

        Thread.sleep(100);

        // We should not receive response for 1st producer, since it was cancelled by the close
        assertNull(observer.pollOneMessage());

        commandSend2.onCompleted();
        observer2.waitForCompletion();
    }

    @Test(timeOut = 30000, invocationCount = 1, skipFailedInvocations = true)
    public void testCreateProducerBookieTimeout() throws Exception {
        // Delay the topic creation in a deterministic way
        CompletableFuture<Runnable> openFailedTopic = new CompletableFuture<>();
        doAnswer(invocationOnMock -> {
            openFailedTopic.complete(() -> {
                ((AsyncCallbacks.OpenLedgerCallback) invocationOnMock.getArguments()[2])
                        .openLedgerComplete(ledgerMock, null);
            });
            return null;
        }).when(mlFactoryMock).asyncOpen(matches(".*fail.*"), any(ManagedLedgerConfig.class),
                any(AsyncCallbacks.OpenLedgerCallback.class), any(Supplier.class), any());

        // In a create producer timeout from client side we expect to see this sequence of commands :
        // 1. create a failure producer which will timeout creation after 100msec
        // 2. close producer
        // 3. Recreate producer (triggered by reconnection logic)
        // 4. Wait till the timeout of 1, and create producer again.

        // These operations need to be serialized, to allow the last create producer to finally succeed
        // (There can be more create/close pairs in the sequence, depending on the client timeout

        String producerName = "my-producer";

        CommandProducer createProducer1 = Commands.newProducer(failTopicName, producerName, Collections.emptyMap());
        PulsarGrpc.PulsarStub produceStub = Commands.attachProducerParams(stub, createProducer1);

        TestStreamObserver<SendResult> observer = TestStreamObserver.create();
        StreamObserver<CommandSend> commandSend1 = produceStub.produce(observer);

        commandSend1.onCompleted();

        CommandProducer createProducer2 = Commands.newProducer(successTopicName, producerName, Collections.emptyMap());
        produceStub = Commands.attachProducerParams(stub, createProducer2);
        TestStreamObserver<SendResult> observer2 = TestStreamObserver.create();
        StreamObserver<CommandSend> commandSend2 = produceStub.produce(observer2);

        // Now the topic gets opened.. It will make 2nd producer creation successful
        openFailedTopic.get().run();

        // Close succeeds
        observer.waitForCompletion();

        // 2nd producer success as topic is opened
        assertTrue(observer2.takeOneMessage().hasProducerSuccess());

        // Wait till the failtopic timeout interval
        Thread.sleep(500);

        // 3rd producer fails because 2nd is already connected
        verifyProduceFails(createProducer2, Status.FAILED_PRECONDITION, ServerError.ProducerBusy);
        Thread.sleep(500);

        // We should not receive response for 1st producer, since it was cancelled by the close
        assertNull(observer.pollOneMessage());

        commandSend2.onCompleted();
        observer2.waitForCompletion();
    }

    @Test
    public void testSubscribeTimeout() throws Exception {
        // Delay the topic creation in a deterministic way
        CompletableFuture<Runnable> openTopicTask = new CompletableFuture<>();
        doAnswer(invocationOnMock -> {
            openTopicTask.complete(() -> {
                ((AsyncCallbacks.OpenLedgerCallback) invocationOnMock.getArguments()[2])
                        .openLedgerComplete(ledgerMock, null);
            });

            return null;
        }).when(mlFactoryMock).asyncOpen(matches(".*success.*"), any(ManagedLedgerConfig.class),
                any(AsyncCallbacks.OpenLedgerCallback.class), any(Supplier.class), any());

        // In a subscribe timeout from client side we expect to see this sequence of commands :
        // 1. Subscribe
        // 2. close consumer (when the timeout is triggered, which may be before the consumer was created on the broker)
        // 3. Subscribe (triggered by reconnection logic)

        // These operations need to be serialized, to allow the last subscribe operation to finally succeed
        // (There can be more subscribe/close pairs in the sequence, depending on the client timeout

        CommandSubscribe subscribe = Commands.newSubscribe(successTopicName, successSubName, SubType.Exclusive, 0,
                "test" /* consumer name */, 0 /* avoid reseting cursor */);
        PulsarGrpc.PulsarStub consumerStub = Commands.attachConsumerParams(stub, subscribe);

        TestStreamObserver<ConsumeOutput> observer = TestStreamObserver.create();
        StreamObserver<ConsumeInput> consumeInput = consumerStub.consume(observer);

        consumeInput.onCompleted();

        Thread.sleep(100);

        TestStreamObserver<ConsumeOutput> observer2 = TestStreamObserver.create();
        StreamObserver<ConsumeInput> consumeInput2 = consumerStub.consume(observer2);

        openTopicTask.get().run();

        // Close succeeds
        observer.waitForErrorOrCompletion();

        // 2nd producer success as topic is opened
        assertTrue(observer2.takeOneMessage().hasSubscribeSuccess());

        // We should not receive response for 1st consumer, since it was cancelled by the close
        assertNull(observer.pollOneMessage());

        consumeInput2.onCompleted();
        observer2.waitForCompletion();
    }

    @Test
    public void testSubscribeBookieTimeout() throws Exception {
        // Delay the topic creation in a deterministic way
        CompletableFuture<Runnable> openTopicSuccess = new CompletableFuture<>();
        doAnswer(invocationOnMock -> {
            openTopicSuccess.complete(() -> {
                ((AsyncCallbacks.OpenLedgerCallback) invocationOnMock.getArguments()[2])
                        .openLedgerComplete(ledgerMock, null);
            });
            return null;
        }).when(mlFactoryMock).asyncOpen(matches(".*success.*"), any(ManagedLedgerConfig.class),
                any(AsyncCallbacks.OpenLedgerCallback.class), any(Supplier.class), any());

        CompletableFuture<Runnable> openTopicFail = new CompletableFuture<>();
        doAnswer(invocationOnMock -> {
            openTopicFail.complete(() -> {
                ((AsyncCallbacks.OpenLedgerCallback) invocationOnMock.getArguments()[2])
                        .openLedgerFailed(new ManagedLedgerException("Managed ledger failure"), null);
            });
            return null;
        }).when(mlFactoryMock).asyncOpen(matches(".*fail.*"), any(ManagedLedgerConfig.class),
                any(AsyncCallbacks.OpenLedgerCallback.class), any(Supplier.class), any());

        // In a subscribe timeout from client side we expect to see this sequence of commands :
        // 1. Subscribe against failtopic which will fail after 100msec
        // 2. close consumer
        // 3. Resubscribe (triggered by reconnection logic)
        // 4. Wait till the timeout of 1, and subscribe again.

        // These operations need to be serialized, to allow the last subscribe operation to finally succeed
        // (There can be more subscribe/close pairs in the sequence, depending on the client timeout
        CommandSubscribe subscribe = Commands.newSubscribe(failTopicName, successSubName, SubType.Exclusive, 0,
                "test" /* consumer name */, 0 /* avoid reseting cursor */);
        PulsarGrpc.PulsarStub consumerStub = Commands.attachConsumerParams(stub, subscribe);
        TestStreamObserver<ConsumeOutput> observer = TestStreamObserver.create();
        StreamObserver<ConsumeInput> consumeInput = consumerStub.consume(observer);

        consumeInput.onCompleted();

        subscribe = Commands.newSubscribe(successTopicName, successSubName, SubType.Exclusive, 0,
                "test" /* consumer name */, 0 /* avoid reseting cursor */);
        consumerStub = Commands.attachConsumerParams(stub, subscribe);
        TestStreamObserver<ConsumeOutput> observer2 = TestStreamObserver.create();
        StreamObserver<ConsumeInput> consumeInput2 = consumerStub.consume(observer2);

        openTopicFail.get().run();

        // Subscribe fails
        assertErrorIsStatusExceptionWithServerError(observer.waitForError(), Status.FAILED_PRECONDITION,
                ServerError.PersistenceError);

        openTopicSuccess.get().run();

        // Subscribe succeeds
        assertTrue(observer2.takeOneMessage().hasSubscribeSuccess());

        Thread.sleep(100);

        // We should not receive response for 1st consumer, since it was cancelled by the close
        assertNull(observer.pollOneMessage());

        consumeInput2.onCompleted();
        observer2.waitForCompletion();
    }

    @Test
    public void testSubscribeCommand() throws Exception {
        final String failSubName = "failSub";

        doReturn(false).when(brokerService).isAuthenticationEnabled();
        doReturn(false).when(brokerService).isAuthorizationEnabled();
        // test SUBSCRIBE on topic and cursor creation success
        CommandSubscribe subscribe = Commands.newSubscribe(successTopicName, successSubName, SubType.Exclusive, 0,
                "test" /* consumer name */, 0 /* avoid reseting cursor */);
        PulsarGrpc.PulsarStub consumerStub = Commands.attachConsumerParams(stub, subscribe);

        TestStreamObserver<ConsumeOutput> observer = TestStreamObserver.create();
        StreamObserver<ConsumeInput> consumeInput = consumerStub.consume(observer);
        assertTrue(observer.takeOneMessage().hasSubscribeSuccess());

        PersistentTopic topicRef = (PersistentTopic) brokerService.getTopicReference(successTopicName).get();

        assertNotNull(topicRef);
        assertTrue(topicRef.getSubscriptions().containsKey(successSubName));
        assertTrue(topicRef.getSubscription(successSubName).getDispatcher().isConsumerConnected());

        consumeInput.onCompleted();
        observer.waitForCompletion();

        // test SUBSCRIBE on topic creation success and cursor failure
        subscribe = Commands.newSubscribe(successTopicName, failSubName, SubType.Exclusive, 0,
                "test" /* consumer name */, 0 /* avoid reseting cursor */);
        verifyConsumeFails(subscribe, Status.FAILED_PRECONDITION, ServerError.PersistenceError);

        // test SUBSCRIBE on topic creation failure
        subscribe = Commands.newSubscribe(failTopicName, successSubName, SubType.Exclusive, 0,
                "test" /* consumer name */, 0 /* avoid reseting cursor */);
        verifyConsumeFails(subscribe, Status.FAILED_PRECONDITION, ServerError.PersistenceError);
    }

    @Test
    public void testSubscribeCommandWithAuthorizationPositive() throws Exception {
        AuthorizationService authorizationService = mock(AuthorizationService.class);
        doReturn(CompletableFuture.completedFuture(true)).when(authorizationService)
                .allowTopicOperationAsync(Mockito.any(),
                        Mockito.any(), Mockito.any(), Mockito.any());
        doReturn(authorizationService).when(brokerService).getAuthorizationService();
        doReturn(true).when(brokerService).isAuthenticationEnabled();
        doReturn(true).when(brokerService).isAuthorizationEnabled();

        // test SUBSCRIBE on topic and cursor creation success
        CommandSubscribe subscribe = Commands.newSubscribe(successTopicName, successSubName, SubType.Exclusive, 0,
                "test" /* consumer name */, 0 /* avoid reseting cursor */);
        PulsarGrpc.PulsarStub consumerStub = Commands.attachConsumerParams(stub, subscribe);

        TestStreamObserver<ConsumeOutput> observer = TestStreamObserver.create();
        StreamObserver<ConsumeInput> consumeInput = consumerStub.consume(observer);
        assertTrue(observer.takeOneMessage().hasSubscribeSuccess());

        consumeInput.onCompleted();
        observer.waitForCompletion();
    }

    @Test
    public void testSubscribeCommandWithAuthorizationNegative() throws Exception {
        AuthorizationService authorizationService = mock(AuthorizationService.class);
        doReturn(CompletableFuture.completedFuture(false)).when(authorizationService)
                .allowTopicOperationAsync(Mockito.any(),
                        Mockito.any(), Mockito.any(), Mockito.any());
        doReturn(authorizationService).when(brokerService).getAuthorizationService();
        doReturn(true).when(brokerService).isAuthenticationEnabled();
        doReturn(true).when(brokerService).isAuthorizationEnabled();

        // test SUBSCRIBE on topic and cursor creation success
        CommandSubscribe subscribe = Commands.newSubscribe(successTopicName, successSubName, SubType.Exclusive, 0,
                "test" /* consumer name */, 0 /* avoid reseting cursor */);
        verifyConsumeFails(subscribe, Status.PERMISSION_DENIED, ServerError.AuthorizationError);
    }

    @Test
    public void testAckCommand() throws Exception {
        PositionImpl pos = new PositionImpl(0, 0);
        doReturn(pos).when(cursorMock).getMarkDeletedPosition();
        CommandSubscribe subscribe = Commands.newSubscribe(successTopicName, successSubName, SubType.Exclusive, 0,
                "test" /* consumer name */, 0 /* avoid reseting cursor */);
        PulsarGrpc.PulsarStub consumerStub = Commands.attachConsumerParams(stub, subscribe);

        TestStreamObserver<ConsumeOutput> observer = TestStreamObserver.create();
        StreamObserver<ConsumeInput> consumeInput = consumerStub.consume(observer);
        assertTrue(observer.takeOneMessage().hasSubscribeSuccess());

        consumeInput.onNext(Commands.newAck(pos.getLedgerId(), pos.getEntryId(), null, AckType.Individual,
                null, Collections.emptyMap(), -1, -1, 1, -1));

        ConsumeOutput consumeOutput = observer.takeOneMessage();
        assertTrue(consumeOutput.hasAckResponse());
        CommandAckResponse ackResponse = consumeOutput.getAckResponse();
        assertFalse(ackResponse.hasError());
        assertFalse(ackResponse.hasMessage());
        consumeInput.onCompleted();
        observer.waitForCompletion();
    }

    //@Test
    public void testAckCommandError() throws Exception {
        pulsar.getConfiguration().setTransactionCoordinatorEnabled(false);
        PositionImpl pos = new PositionImpl(0, 0);
        doReturn(pos).when(cursorMock).getMarkDeletedPosition();
        CommandSubscribe subscribe = Commands.newSubscribe(successTopicName, successSubName, SubType.Exclusive, 0,
                "test" /* consumer name */, 0 /* avoid reseting cursor */);
        PulsarGrpc.PulsarStub consumerStub = Commands.attachConsumerParams(stub, subscribe);

        TestStreamObserver<ConsumeOutput> observer = TestStreamObserver.create();
        StreamObserver<ConsumeInput> consumeInput = consumerStub.consume(observer);
        assertTrue(observer.takeOneMessage().hasSubscribeSuccess());

        consumeInput.onNext(Commands.newAck(pos.getLedgerId(), pos.getEntryId(), null, AckType.Individual,
                null, Collections.emptyMap(), 100, 100, 1, -1));

        ConsumeOutput consumeOutput = observer.takeOneMessage();
        assertTrue(consumeOutput.hasAckResponse());
        CommandAckResponse ackResponse = consumeOutput.getAckResponse();
        assertEquals(ackResponse.getError(), ServerError.NotAllowedError);
        consumeInput.onCompleted();
        observer.waitForCompletion();
    }

    @Test
    public void testFlowCommand() throws Exception {
        CommandSubscribe subscribe = Commands.newSubscribe(successTopicName, successSubName, SubType.Exclusive, 0,
                "test" /* consumer name */, 0 /* avoid reseting cursor */);
        PulsarGrpc.PulsarStub consumerStub = Commands.attachConsumerParams(stub, subscribe);

        TestStreamObserver<ConsumeOutput> observer = TestStreamObserver.create();
        StreamObserver<ConsumeInput> consumeInput = consumerStub.consume(observer);
        assertTrue(observer.takeOneMessage().hasSubscribeSuccess());

        consumeInput.onNext(Commands.newFlow(1));

        // verify nothing is sent out on the wire after ack
        Thread.sleep(100);
        assertNull(observer.pollOneMessage());
        consumeInput.onCompleted();
        observer.waitForCompletion();
    }

    @Test
    public void testProducerSuccessOnEncryptionRequiredTopic() throws Exception {
        // Set encryption_required to true
        ZooKeeperDataCache<Policies> zkDataCache = mock(ZooKeeperDataCache.class);
        Policies policies = mock(Policies.class);
        policies.encryption_required = true;
        policies.topicDispatchRate = Maps.newHashMap();
        policies.clusterDispatchRate = Maps.newHashMap();
        doReturn(Optional.of(policies)).when(zkDataCache)
                .get(AdminResource.path(POLICIES, TopicName.get(encryptionRequiredTopicName).getNamespace()));
        doReturn(CompletableFuture.completedFuture(Optional.of(policies))).when(zkDataCache)
                .getAsync(AdminResource.path(POLICIES, TopicName.get(encryptionRequiredTopicName).getNamespace()));
        doReturn(zkDataCache).when(configCacheService).policiesCache();

        // test success case: encrypted producer can connect
        CommandProducer producerParams = Commands.newProducer(encryptionRequiredTopicName,
                "encrypted-producer", true, null);
        PulsarGrpc.PulsarStub producerStub = Commands.attachProducerParams(stub, producerParams);

        TestStreamObserver<SendResult> observer = TestStreamObserver.create();
        StreamObserver<CommandSend> produce = producerStub.produce(observer);

        SendResult sendResult = observer.takeOneMessage();

        assertTrue(sendResult.hasProducerSuccess());
        PersistentTopic topicRef = (PersistentTopic) brokerService.getTopicReference(encryptionRequiredTopicName).get();
        assertNotNull(topicRef);
        assertEquals(topicRef.getProducers().size(), 1);

        produce.onCompleted();
        observer.waitForCompletion();
    }

    @Test
    public void testProducerFailureOnEncryptionRequiredTopic() throws Exception {
        // Set encryption_required to true
        ZooKeeperDataCache<Policies> zkDataCache = mock(ZooKeeperDataCache.class);
        Policies policies = mock(Policies.class);
        policies.encryption_required = true;
        policies.topicDispatchRate = Maps.newHashMap();
        policies.clusterDispatchRate = Maps.newHashMap();
        doReturn(Optional.of(policies)).when(zkDataCache)
                .get(AdminResource.path(POLICIES, TopicName.get(encryptionRequiredTopicName).getNamespace()));
        doReturn(CompletableFuture.completedFuture(Optional.of(policies))).when(zkDataCache)
                .getAsync(AdminResource.path(POLICIES, TopicName.get(encryptionRequiredTopicName).getNamespace()));
        doReturn(zkDataCache).when(configCacheService).policiesCache();

        // test success case: encrypted producer can connect
        CommandProducer producerParams = Commands.newProducer(encryptionRequiredTopicName,
                "unencrypted-producer", false, null);
        verifyProduceFails(producerParams, Status.INVALID_ARGUMENT, ServerError.MetadataError);
        PersistentTopic topicRef = (PersistentTopic) brokerService.getTopicReference(encryptionRequiredTopicName).get();
        assertNotNull(topicRef);
        assertEquals(topicRef.getProducers().size(), 0);
    }

    @Test
    public void testProducerFailureOnEncryptionRequiredOnBroker() throws Exception {
        // (a) Set encryption-required at broker level
        svcConfig.setEncryptionRequireOnProducer(true);

        // (b) Set encryption_required to false on policy
        ZooKeeperDataCache<Policies> zkDataCache = mock(ZooKeeperDataCache.class);
        Policies policies = mock(Policies.class);
        policies.encryption_required = false;
        policies.topicDispatchRate = Maps.newHashMap();
        policies.clusterDispatchRate = Maps.newHashMap();
        doReturn(Optional.of(policies)).when(zkDataCache)
                .get(AdminResource.path(POLICIES, TopicName.get(encryptionRequiredTopicName).getNamespace()));
        doReturn(CompletableFuture.completedFuture(Optional.of(policies))).when(zkDataCache)
                .getAsync(AdminResource.path(POLICIES, TopicName.get(encryptionRequiredTopicName).getNamespace()));
        doReturn(zkDataCache).when(configCacheService).policiesCache();

        // test success case: encrypted producer can connect
        CommandProducer producerParams = Commands.newProducer(encryptionRequiredTopicName,
                "unencrypted-producer", false, null);
        verifyProduceFails(producerParams, Status.INVALID_ARGUMENT, ServerError.MetadataError);
        PersistentTopic topicRef = (PersistentTopic) brokerService.getTopicReference(encryptionRequiredTopicName).get();
        assertNotNull(topicRef);
        assertEquals(topicRef.getProducers().size(), 0);
    }

    @Test
    public void testSendSuccessOnEncryptionRequiredTopic() throws Exception {
        // Set encryption_required to true
        ZooKeeperDataCache<Policies> zkDataCache = mock(ZooKeeperDataCache.class);
        Policies policies = mock(Policies.class);
        policies.encryption_required = true;
        policies.topicDispatchRate = Maps.newHashMap();
        policies.clusterDispatchRate = Maps.newHashMap();
        doReturn(Optional.of(policies)).when(zkDataCache)
                .get(AdminResource.path(POLICIES, TopicName.get(encryptionRequiredTopicName).getNamespace()));
        doReturn(CompletableFuture.completedFuture(Optional.of(policies))).when(zkDataCache)
                .getAsync(AdminResource.path(POLICIES, TopicName.get(encryptionRequiredTopicName).getNamespace()));
        doReturn(zkDataCache).when(configCacheService).policiesCache();

        CommandProducer producerParams = Commands.newProducer(encryptionRequiredTopicName,
                "prod-name", true, null);

        PulsarGrpc.PulsarStub producerStub = Commands.attachProducerParams(stub, producerParams);

        TestStreamObserver<SendResult> observer = TestStreamObserver.create();
        StreamObserver<CommandSend> produce = producerStub.produce(observer);

        assertTrue(observer.takeOneMessage().hasProducerSuccess());

        // test success case: encrypted messages can be published
        MessageMetadata messageMetadata = new MessageMetadata()
                .setPublishTime(System.currentTimeMillis())
                .setProducerName("prod-name")
                .setSequenceId(0);
        messageMetadata.addEncryptionKey()
                .setKey("testKey")
                .setValue("testVal".getBytes());
        ByteBuf data = Unpooled.buffer(1024);

        CommandSend send = Commands.newSend(1, 0, 1, messageMetadata, data);
        produce.onNext(send);

        assertTrue(observer.takeOneMessage().hasSendReceipt());

        produce.onCompleted();
        observer.waitForCompletion();
    }

    @Test
    public void testSendFailureOnEncryptionRequiredTopic() throws Exception {
        // Set encryption_required to true
        ZooKeeperDataCache<Policies> zkDataCache = mock(ZooKeeperDataCache.class);
        Policies policies = mock(Policies.class);
        policies.encryption_required = true;
        policies.topicDispatchRate = Maps.newHashMap();
        policies.clusterDispatchRate = Maps.newHashMap();
        doReturn(Optional.of(policies)).when(zkDataCache)
                .get(AdminResource.path(POLICIES, TopicName.get(encryptionRequiredTopicName).getNamespace()));
        doReturn(CompletableFuture.completedFuture(Optional.of(policies))).when(zkDataCache)
                .getAsync(AdminResource.path(POLICIES, TopicName.get(encryptionRequiredTopicName).getNamespace()));
        doReturn(zkDataCache).when(configCacheService).policiesCache();

        CommandProducer producerParams = Commands.newProducer(encryptionRequiredTopicName,
                "prod-name", true, null);

        PulsarGrpc.PulsarStub producerStub = Commands.attachProducerParams(stub, producerParams);

        TestStreamObserver<SendResult> observer = TestStreamObserver.create();
        StreamObserver<CommandSend> produce = producerStub.produce(observer);

        assertTrue(observer.takeOneMessage().hasProducerSuccess());

        // test success case: encrypted messages can be published
        MessageMetadata messageMetadata = new MessageMetadata()
                .setPublishTime(System.currentTimeMillis())
                .setProducerName("prod-name")
                .setSequenceId(0);
        ByteBuf data = Unpooled.buffer(1024);

        CommandSend send = Commands.newSend(1, 0, 1, messageMetadata, data);
        produce.onNext(send);

        SendResult sendResult = observer.takeOneMessage();
        assertTrue(sendResult.hasSendError());
        assertEquals(sendResult.getSendError().getError(), ServerError.MetadataError);

        produce.onCompleted();
        observer.waitForCompletion();

        assertTrue(true);
    }

    @Test
    public void testUnauthorizedTopicOnLookup() throws Exception {
        AuthorizationService authorizationService = mock(AuthorizationService.class);
        doReturn(CompletableFuture.completedFuture(false)).when(authorizationService)
                .allowTopicOperationAsync(Mockito.any(),
                        Mockito.any(), Mockito.any(), Mockito.any());
        doReturn(authorizationService).when(brokerService).getAuthorizationService();
        doReturn(true).when(brokerService).isAuthenticationEnabled();
        doReturn(true).when(brokerService).isAuthorizationEnabled();

        try {
            blockingStub.lookupTopic(Commands.newLookup(successTopicName, false));
            fail("StatusRuntimeException should have been thrown");
        } catch (StatusRuntimeException e) {
            assertErrorIsStatusExceptionWithServerError(e, Status.PERMISSION_DENIED,
                    ServerError.AuthorizationError);
        }

    }

    @Test
    public void testInvalidTopicOnLookup() throws Exception {
        String invalidTopicName = "xx/ass/aa/aaa";
        try {
            blockingStub.lookupTopic(Commands.newLookup(invalidTopicName, false));
            fail("StatusRuntimeException should have been thrown");
        } catch (StatusRuntimeException e) {
            assertErrorIsStatusExceptionWithServerError(e, Status.INVALID_ARGUMENT,
                    ServerError.InvalidTopicName);
        }

    }

    @Test
    public void testUnauthorizedTopicOnGetPartitionMetadata() throws Exception {
        AuthorizationService authorizationService = mock(AuthorizationService.class);
        doReturn(CompletableFuture.completedFuture(false)).when(authorizationService)
                .allowTopicOperationAsync(Mockito.any(),
                        Mockito.any(), Mockito.any(), Mockito.any());
        doReturn(authorizationService).when(brokerService).getAuthorizationService();
        doReturn(true).when(brokerService).isAuthenticationEnabled();
        doReturn(true).when(brokerService).isAuthorizationEnabled();

        try {
            blockingStub.getPartitionMetadata(Commands.newPartitionMetadataRequest(successTopicName));
            fail("StatusRuntimeException should have been sent");
        } catch (StatusRuntimeException e) {
            assertErrorIsStatusExceptionWithServerError(e, Status.PERMISSION_DENIED,
                    ServerError.AuthorizationError);
        }
    }

    @Test
    public void testInvalidTopicOnGetPartitionMetadata() throws Exception {
        String invalidTopicName = "xx/ass/aa/aaa";
        try {
            blockingStub.getPartitionMetadata(Commands.newPartitionMetadataRequest(invalidTopicName));
            fail("StatusRuntimeException should have been sent");
        } catch (StatusRuntimeException e) {
            assertErrorIsStatusExceptionWithServerError(e, Status.INVALID_ARGUMENT,
                    ServerError.InvalidTopicName);
        }
    }

    @Test
    public void testInvalidTopicOnProducer() throws Exception {
        String invalidTopicName = "xx/ass/aa/aaa";

        CommandProducer producerParams = Commands.newProducer(invalidTopicName, "prod-name", Collections.emptyMap());
        verifyProduceFails(producerParams, Status.INVALID_ARGUMENT, ServerError.InvalidTopicName);
    }

    @Test
    public void testInvalidTopicOnSubscribe() throws Exception {
        String invalidTopicName = "xx/ass/aa/aaa";

        CommandSubscribe subscribe = Commands.newSubscribe(invalidTopicName, "test-subscription", SubType.Exclusive, 0,
                "consumerName", 0 /*avoid reseting cursor*/);
        verifyConsumeFails(subscribe, Status.INVALID_ARGUMENT, ServerError.InvalidTopicName);
    }

    @Test
    public void testDelayedClosedProducer() throws Exception {
        CompletableFuture<Topic> delayFuture = new CompletableFuture<>();
        Topic topic = spy(new NonPersistentTopic(successTopicName, brokerService));
        doReturn(delayFuture, CompletableFuture.completedFuture(topic)).when(brokerService)
                .getOrCreateTopic(any(String.class));
        // Create producer first time
        CommandProducer createProducer1 = Commands.newProducer(successTopicName, "prod-name", Collections.emptyMap());
        PulsarGrpc.PulsarStub produceStub = Commands.attachProducerParams(stub, createProducer1);

        TestStreamObserver<SendResult> observer = TestStreamObserver.create();
        StreamObserver<CommandSend> commandSend1 = produceStub.produce(observer);

        Thread.sleep(100);

        commandSend1.onCompleted();

        TestStreamObserver<SendResult> observer2 = TestStreamObserver.create();
        StreamObserver<CommandSend> commandSend2 = produceStub.produce(observer2);

        assertTrue(observer2.takeOneMessage().hasProducerSuccess());

        commandSend2.onCompleted();
        observer2.waitForCompletion();

        delayFuture.complete(topic);
        observer.waitForCompletion();
    }

    @Test
    public void testProducerValidationEnforced() throws Exception {
        Topic spyTopic = spy(new NonPersistentTopic(successTopicName, brokerService));
        doReturn(CompletableFuture.completedFuture(true)).when(spyTopic).hasSchema();
        doReturn(true).when(spyTopic).getSchemaValidationEnforced();
        doReturn(CompletableFuture.completedFuture(spyTopic)).when(brokerService).getOrCreateTopic(successTopicName);

        CommandProducer producerParams = Commands.newProducer(successTopicName, "prod-name", Collections.emptyMap());
        verifyProduceFails(producerParams, Status.FAILED_PRECONDITION, ServerError.IncompatibleSchema);
    }

    @Test
    public void testProducerProducerBlockedQuotaExceededErrorOnBacklogQuotaExceeded() throws Exception {
        Topic spyTopic = spy(new PersistentTopic(successTopicName, ledgerMock, brokerService));
        doReturn(true).when(spyTopic)
            .isBacklogQuotaExceeded("exceeded-producer", BacklogQuota.BacklogQuotaType.destination_storage);
        doReturn(new BacklogQuotaImpl(0, 0, BacklogQuota.RetentionPolicy.producer_request_hold))
            .when(spyTopic).getBacklogQuota(BacklogQuota.BacklogQuotaType.destination_storage);
        doReturn(CompletableFuture.completedFuture(spyTopic)).when(brokerService).getOrCreateTopic(successTopicName);

        // test success case: encrypted producer can connect
        CommandProducer producerParams = Commands.newProducer(successTopicName,
                "exceeded-producer", true, null);
        verifyProduceFails(producerParams, Status.FAILED_PRECONDITION, ServerError.ProducerBlockedQuotaExceededError);
    }

    @Test
    public void testProducerProducerBlockedQuotaExceededExceptionOnBacklogQuotaExceeded() throws Exception {
        Topic spyTopic = spy(new PersistentTopic(successTopicName, ledgerMock, brokerService));
        doReturn(true).when(spyTopic)
            .isBacklogQuotaExceeded("exceeded-producer", BacklogQuota.BacklogQuotaType.destination_storage);
        doReturn(new BacklogQuotaImpl(0, 0, BacklogQuota.RetentionPolicy.producer_exception))
            .when(spyTopic).getBacklogQuota(BacklogQuota.BacklogQuotaType.destination_storage);
        doReturn(CompletableFuture.completedFuture(spyTopic)).when(brokerService).getOrCreateTopic(successTopicName);

        // test success case: encrypted producer can connect
        CommandProducer producerParams = Commands.newProducer(successTopicName,
                "exceeded-producer", true, null);
        verifyProduceFails(producerParams, Status.FAILED_PRECONDITION,
                ServerError.ProducerBlockedQuotaExceededException);
    }

    @Test
    public void testProducerWithSchema() throws Exception {
        LongSchemaVersion schemaVersion = new LongSchemaVersion(42L);
        Map<String, String> schemaProps = new HashMap<>();
        schemaProps.put("key0", "value0");
        SchemaInfo schemaInfo = SchemaInfo.builder()
                .name("my-schema")
                .type(SchemaType.STRING)
                .schema("test".getBytes(StandardCharsets.UTF_8))
                .properties(schemaProps)
                .build();

        Topic spyTopic = spy(new NonPersistentTopic(successTopicName, brokerService));
        ArgumentCaptor<SchemaData> schemaCaptor = ArgumentCaptor.forClass(SchemaData.class);
        doReturn(CompletableFuture.completedFuture(schemaVersion)).when(spyTopic).addSchema(schemaCaptor.capture());
        doReturn(CompletableFuture.completedFuture(spyTopic)).when(brokerService).getOrCreateTopic(successTopicName);

        String producerName = "my-producer";
        CommandProducer producerParams = Commands.newProducer(successTopicName, producerName, false,
                Collections.emptyMap(), schemaInfo, 0, false);
        PulsarGrpc.PulsarStub producerStub = Commands.attachProducerParams(stub, producerParams);

        TestStreamObserver<SendResult> observer = TestStreamObserver.create();
        StreamObserver<CommandSend> produce = producerStub.produce(observer);

        SendResult sendResult = observer.takeOneMessage();
        assertTrue(sendResult.hasProducerSuccess());
        assertEquals(sendResult.getProducerSuccess().getSchemaVersion().toByteArray(), schemaVersion.bytes());
        SchemaData schemaData = schemaCaptor.getValue();
        assertEquals(schemaData.getType(), SchemaType.STRING);
        assertEquals(schemaData.getProps().get("key0"), "value0");
        assertEquals(schemaData.getData(), "test".getBytes(StandardCharsets.UTF_8));

        produce.onCompleted();
        observer.waitForCompletion();
    }

    @Test
    public void testActiveConsumerChange() throws Exception {
        // test SUBSCRIBE on topic and cursor creation success
        CommandSubscribe subscribe = Commands.newSubscribe(successTopicName, successSubName, SubType.Failover, 0,
                "test" /* consumer name */, 0 /* avoid reseting cursor */);
        PulsarGrpc.PulsarStub consumerStub = Commands.attachConsumerParams(stub, subscribe);

        TestStreamObserver<ConsumeOutput> observer = TestStreamObserver.create();
        StreamObserver<ConsumeInput> consumeInput = consumerStub.consume(observer);
        assertTrue(observer.takeOneMessage().hasSubscribeSuccess());

        TestStreamObserver<ConsumeOutput> observer2 = TestStreamObserver.create();
        StreamObserver<ConsumeInput> consumeInput2 = consumerStub.consume(observer2);

        ConsumeOutput consumeOutput = observer.takeOneMessage();
        assertTrue(consumeOutput.hasActiveConsumerChange());
        assertTrue(consumeOutput.getActiveConsumerChange().getIsActive());

        consumeOutput = observer2.takeOneMessage();
        assertTrue(consumeOutput.hasActiveConsumerChange());
        assertFalse(consumeOutput.getActiveConsumerChange().getIsActive());

        assertTrue(observer2.takeOneMessage().hasSubscribeSuccess());

        consumeInput.onCompleted();
        observer.waitForCompletion();

        consumeInput2.onCompleted();
        observer2.waitForCompletion();
    }

    @Test
    public void testReachEndOfTopic() throws Exception {
        PositionImpl pos = new PositionImpl(0, 0);
        doReturn(pos).when(cursorMock).getMarkDeletedPosition();
        CommandSubscribe subscribe = Commands.newSubscribe(successTopicName, successSubName, SubType.Exclusive, 0,
                "test" /* consumer name */, 0 /* avoid reseting cursor */);
        PulsarGrpc.PulsarStub consumerStub = Commands.attachConsumerParams(stub, subscribe);

        TestStreamObserver<ConsumeOutput> observer = TestStreamObserver.create();
        StreamObserver<ConsumeInput> consumeInput = consumerStub.consume(observer);
        assertTrue(observer.takeOneMessage().hasSubscribeSuccess());

        doReturn(true).when(ledgerMock).isTerminated();
        doReturn(0L).when(cursorMock).getNumberOfEntriesInBacklog(false);

        consumeInput.onNext(Commands.newAck(pos.getLedgerId(), pos.getEntryId(), AckType.Individual,
                null, Collections.emptyMap()));

        assertTrue(observer.takeOneMessage().hasReachedEndOfTopic());
        consumeInput.onCompleted();
        observer.waitForCompletion();
    }

    @Test
    public void testConsumerStats() throws Exception {
        CommandSubscribe subscribe = Commands.newSubscribe(successTopicName, successSubName, SubType.Exclusive, 0,
                "test" /* consumer name */, 0 /* avoid reseting cursor */);
        PulsarGrpc.PulsarStub consumerStub = Commands.attachConsumerParams(stub, subscribe);

        TestStreamObserver<ConsumeOutput> observer = TestStreamObserver.create();
        StreamObserver<ConsumeInput> consumeInput = consumerStub.consume(observer);
        assertTrue(observer.takeOneMessage().hasSubscribeSuccess());

        consumeInput.onNext(Commands.newConsumerStats(1L));
        ConsumeOutput consumeOutput = observer.takeOneMessage();

        assertTrue(consumeOutput.hasConsumerStatsResponse());
        CommandConsumerStatsResponse consumerStatsResponse = consumeOutput.getConsumerStatsResponse();
        assertEquals(consumerStatsResponse.getRequestId(), 1L);
        assertEquals(consumerStatsResponse.getConsumerName(), "test");

        consumeInput.onCompleted();
        observer.waitForCompletion();
    }

    @Test
    public void testConsumerStatsError() throws Exception {
        // Delay the topic creation in a deterministic way
        CompletableFuture<Runnable> openTopicTask = new CompletableFuture<>();
        doAnswer(invocationOnMock -> {
            openTopicTask.complete(() -> {
                ((AsyncCallbacks.OpenLedgerCallback) invocationOnMock.getArguments()[2])
                        .openLedgerComplete(ledgerMock, null);
            });

            return null;
        }).when(mlFactoryMock).asyncOpen(matches(".*success.*"), any(ManagedLedgerConfig.class),
                any(AsyncCallbacks.OpenLedgerCallback.class), any(Supplier.class), any());

        CommandSubscribe subscribe = Commands.newSubscribe(successTopicName, successSubName, SubType.Exclusive, 0,
                "test" /* consumer name */, 0 /* avoid reseting cursor */);
        PulsarGrpc.PulsarStub consumerStub = Commands.attachConsumerParams(stub, subscribe);

        TestStreamObserver<ConsumeOutput> observer = TestStreamObserver.create();
        StreamObserver<ConsumeInput> consumeInput = consumerStub.consume(observer);

        consumeInput.onNext(Commands.newConsumerStats(1L));
        ConsumeOutput consumeOutput = observer.takeOneMessage();

        assertTrue(consumeOutput.hasError());
        assertEquals(consumeOutput.getError().getRequestId(), 1L);

        openTopicTask.get().run();

        assertTrue(observer.takeOneMessage().hasSubscribeSuccess());

        consumeInput.onCompleted();
        observer.waitForCompletion();
    }

    @Test
    public void testUnsubscribeSuccess() throws Exception {
        CommandSubscribe subscribe = Commands.newSubscribe(successTopicName, successSubName, SubType.Exclusive, 0,
                "test" /* consumer name */, 0 /* avoid reseting cursor */);
        PulsarGrpc.PulsarStub consumerStub = Commands.attachConsumerParams(stub, subscribe);

        TestStreamObserver<ConsumeOutput> observer = TestStreamObserver.create();
        StreamObserver<ConsumeInput> consumeInput = consumerStub.consume(observer);
        assertTrue(observer.takeOneMessage().hasSubscribeSuccess());

        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((AsyncCallbacks.DeleteLedgerCallback) invocationOnMock.getArguments()[1]).deleteLedgerComplete(null);
                return null;
            }
        }).when(mlFactoryMock).asyncDelete(matches(".*success.*"),
            any(AsyncCallbacks.DeleteLedgerCallback.class), any());

        consumeInput.onNext(Commands.newUnsubscribe(1));

        ConsumeOutput consumeOutput = observer.takeOneMessage();
        assertTrue(consumeOutput.hasSuccess());
        assertEquals(consumeOutput.getSuccess().getRequestId(), 1L);

        consumeInput.onCompleted();
        // Here we get an error since the consumer was already removed by the unsubscribed
        observer.waitForError();
    }

    @Test
    public void testUnsubscribeError() throws Exception {
        CommandSubscribe subscribe = Commands.newSubscribe(successTopicName, successSubName, SubType.Shared, 0,
                "test" /* consumer name */, 0 /* avoid reseting cursor */);
        PulsarGrpc.PulsarStub consumerStub = Commands.attachConsumerParams(stub, subscribe);

        TestStreamObserver<ConsumeOutput> observer = TestStreamObserver.create();
        StreamObserver<ConsumeInput> consumeInput = consumerStub.consume(observer);
        assertTrue(observer.takeOneMessage().hasSubscribeSuccess());

        TestStreamObserver<ConsumeOutput> observer2 = TestStreamObserver.create();
        StreamObserver<ConsumeInput> consumeInput2 = consumerStub.consume(observer2);
        assertTrue(observer2.takeOneMessage().hasSubscribeSuccess());

        consumeInput.onNext(Commands.newUnsubscribe(1));
        ConsumeOutput consumeOutput = observer.takeOneMessage();
        assertTrue(consumeOutput.hasError());
        CommandError error = consumeOutput.getError();
        assertEquals(error.getRequestId(), 1L);
        assertEquals(error.getError(), ServerError.MetadataError);

        consumeInput.onCompleted();
        observer.waitForCompletion();
        consumeInput2.onCompleted();
        observer2.waitForCompletion();
    }

    @Test
    public void testGetTopicsOfNamespace() {
        doReturn(CompletableFuture.completedFuture(Arrays.asList("my-topic1", "my-topic2"))).when(namespaceService)
                .getListOfTopics(
                    NamespaceName.get("xx/ass/aa"),
                    org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace.Mode.PERSISTENT);

        CommandGetTopicsOfNamespace request =
                Commands.newGetTopicsOfNamespaceRequest("xx/ass/aa", CommandGetTopicsOfNamespace.Mode.PERSISTENT);
        CommandGetTopicsOfNamespaceResponse topics = blockingStub.getTopicsOfNamespace(request);

        assertEquals(topics.getTopicsList().get(0), "my-topic1");
        assertEquals(topics.getTopicsList().get(1), "my-topic2");
    }

    @Test
    public void testCreateTransaction() {
        long tcId = 100;
        TransactionCoordinatorID coordinatorID = TransactionCoordinatorID.get(tcId);
        transactionMetadataStoreService.addTransactionMetadataStore(coordinatorID);

        Awaitility.await().until(() ->
            transactionMetadataStoreService.getStores().containsKey(coordinatorID));

        CommandNewTxnResponse txn = blockingStub.createTransaction(Commands.newTxn(tcId));

        TxnStatus txnStatus =
                transactionMetadataStoreService.getTxnMeta(new TxnID(txn.getTxnidMostBits(), txn.getTxnidLeastBits()))
                        .thenApply(TxnMeta::status)
                        .getNow(null);

        transactionMetadataStoreService.removeTransactionMetadataStore(coordinatorID);

        assertEquals(txnStatus, TxnStatus.OPEN);
        assertEquals(txn.getTxnidMostBits(), tcId);
        assertEquals(txn.getTxnidLeastBits(), 0);
    }

    @Test
    public void testCreateTransactionError() {
        try {
            blockingStub.createTransaction(CommandNewTxn.getDefaultInstance());
            fail("StatusRuntimeException should have been thrown");
        } catch (StatusRuntimeException e) {
            assertErrorIsStatusExceptionWithServerError(e, Status.FAILED_PRECONDITION,
                    ServerError.TransactionCoordinatorNotFound);
        }
    }

    @Test
    public void testAddPartitionsToTransaction() {
        long tcId = 100;
        TransactionCoordinatorID coordinatorID = TransactionCoordinatorID.get(tcId);
        transactionMetadataStoreService.addTransactionMetadataStore(coordinatorID);

        Awaitility.await().until(() ->
            transactionMetadataStoreService.getStores().containsKey(coordinatorID));

        CommandNewTxnResponse txn = blockingStub.createTransaction(Commands.newTxn(tcId));

        List<String> partitions = Arrays.asList("part1", "part2");
        blockingStub.addPartitionsToTransaction(
                Commands.newAddPartitionToTxn(txn.getTxnidLeastBits(), txn.getTxnidMostBits(), partitions));

        List<String> txnPartitions =
                transactionMetadataStoreService.getTxnMeta(new TxnID(txn.getTxnidMostBits(), txn.getTxnidLeastBits()))
                        .thenApply(TxnMeta::producedPartitions)
                        .getNow(null);

        transactionMetadataStoreService.removeTransactionMetadataStore(coordinatorID);

        assertEquals(txnPartitions, partitions);
    }

    @Test
    public void testAddPartitionsToTransactionError() {
        List<String> partitions = Arrays.asList("part1", "part2");
        try {
            blockingStub.addPartitionsToTransaction(Commands.newAddPartitionToTxn(100, 200, partitions));
            fail("StatusRuntimeException should have been thrown");
        } catch (StatusRuntimeException e) {
            assertErrorIsStatusExceptionWithServerError(e, Status.FAILED_PRECONDITION,
                    ServerError.TransactionCoordinatorNotFound);
        }
    }

    @Test
    public void testEndTransaction() {
        long tcId = 100;
        TransactionCoordinatorID coordinatorID = TransactionCoordinatorID.get(tcId);
        transactionMetadataStoreService.addTransactionMetadataStore(coordinatorID);

        Awaitility.await().until(() ->
            transactionMetadataStoreService.getStores().containsKey(coordinatorID));

        CommandNewTxnResponse txn = blockingStub.createTransaction(Commands.newTxn(tcId));

        CommandEndTxnResponse response = blockingStub.endTransaction(
                Commands.newEndTxn(txn.getTxnidLeastBits(), txn.getTxnidMostBits(), TxnAction.ABORT));

        TxnStatus txnStatus = transactionMetadataStoreService
                .getTxnMeta(new TxnID(txn.getTxnidMostBits(), txn.getTxnidLeastBits()))
                .thenApply(TxnMeta::status)
                .getNow(null);

        transactionMetadataStoreService.removeTransactionMetadataStore(coordinatorID);

        assertEquals(txnStatus, TxnStatus.ABORTED);
        assertEquals(response.getTxnidLeastBits(), 0);
        assertEquals(response.getTxnidMostBits(), 100);
    }

    @Test
    public void testEndTransactionError() {
        try {
            blockingStub.endTransaction(Commands.newEndTxn(100, 200, TxnAction.ABORT));
            fail("StatusRuntimeException should have been thrown");
        } catch (StatusRuntimeException e) {
            assertErrorIsStatusExceptionWithServerError(e, Status.UNKNOWN,
                    ServerError.TransactionCoordinatorNotFound);
        }
    }

    @Test
    public void testEndTransactionOnPartition() {
        Topic spyTopic = mock(PersistentTopic.class);

        doReturn(CompletableFuture.completedFuture(null)).when(spyTopic)
            .endTxn(eq(new TxnID(200, 100)), eq(TxnAction.ABORT_VALUE), eq(150L));
        doReturn(CompletableFuture.completedFuture(Optional.of(spyTopic))).when(brokerService)
            .getTopicIfExists(successTopicName);

        CommandEndTxnOnPartitionResponse response = blockingStub.endTransactionOnPartition(
                Commands.newEndTxnOnPartition(100, 200, successTopicName,
                        TxnAction.ABORT, 150));

        assertEquals(response.getTxnidLeastBits(), 100);
        assertEquals(response.getTxnidMostBits(), 200);
    }

    @Test
    public void testEndTransactionOnPartitionTopicNotFound() {
        doReturn(CompletableFuture.completedFuture(Optional.empty())).when(brokerService)
            .getTopicIfExists(successTopicName);

        doReturn(CompletableFuture.completedFuture(false)).when(mlFactoryMock)
            .asyncExists(TopicName.get(successTopicName).getPersistenceNamingEncoding());

        CommandEndTxnOnPartitionResponse response = blockingStub.endTransactionOnPartition(
            Commands.newEndTxnOnPartition(100, 200, successTopicName,
                TxnAction.ABORT, 150));

        assertEquals(response.getTxnidLeastBits(), 100);
        assertEquals(response.getTxnidMostBits(), 200);
    }

    @Test
    public void testEndTransactionOnPartitionError() {
        // Non persistent topics don't support transactions
        Topic topic = new NonPersistentTopic(successTopicName, brokerService);

        doReturn(CompletableFuture.completedFuture(Optional.of(topic))).when(brokerService)
            .getTopicIfExists(successTopicName);

        try {
            blockingStub.endTransactionOnPartition(Commands.newEndTxnOnPartition(
                    100, 200, successTopicName, TxnAction.ABORT, 150));
            fail("StatusRuntimeException should have been thrown");
        } catch (StatusRuntimeException e) {
            assertErrorIsStatusExceptionWithServerError(e, Status.UNKNOWN, ServerError.UnknownError);
        }
    }

    @Test
    public void testEndTransactionOnSubscription() {
        Subscription subscription = mock(Subscription.class);
        doReturn(CompletableFuture.completedFuture(null))
                .when(subscription).endTxn(200, 100, TxnAction.ABORT_VALUE, 150);

        Topic topic = mock(Topic.class);
        doReturn(subscription).when(topic).getSubscription(successSubName);
        doReturn(CompletableFuture.completedFuture(Optional.of(topic)))
            .when(brokerService).getTopicIfExists(successTopicName);

        CommandEndTxnOnSubscriptionResponse response = blockingStub.endTransactionOnSubscription(
                Commands.newEndTxnOnSubscription(100, 200, successTopicName,
                        successSubName, TxnAction.ABORT, 150));

        assertEquals(response.getTxnidLeastBits(), 100);
        assertEquals(response.getTxnidMostBits(), 200);
    }

    @Test
    public void testEndTransactionOnSubscriptionTopicNotFound() {
        doReturn(CompletableFuture.completedFuture(Optional.empty()))
            .when(brokerService).getTopicIfExists(successTopicName);
        doReturn(CompletableFuture.completedFuture(false))
            .when(mlFactoryMock).asyncExists(TopicName.get(successTopicName).getPersistenceNamingEncoding());

        CommandEndTxnOnSubscriptionResponse response = blockingStub.endTransactionOnSubscription(
            Commands.newEndTxnOnSubscription(100, 200, successTopicName,
                successSubName, TxnAction.ABORT, 150));

        assertEquals(response.getTxnidLeastBits(), 100);
        assertEquals(response.getTxnidMostBits(), 200);
    }

    @Test
    public void testEndTransactionOnSubscriptionSubscriptionNotFound() {
        Topic topic = new NonPersistentTopic(successTopicName, brokerService);
        doReturn(CompletableFuture.completedFuture(Optional.of(topic)))
            .when(brokerService).getTopicIfExists(successTopicName);

        CommandEndTxnOnSubscriptionResponse response = blockingStub.endTransactionOnSubscription(
            Commands.newEndTxnOnSubscription(100, 200, successTopicName,
                successSubName, TxnAction.ABORT, 150));

        assertEquals(response.getTxnidLeastBits(), 100);
        assertEquals(response.getTxnidMostBits(), 200);
    }

    @Test
    public void testEndTransactionOnSubscriptionError() throws Exception {
        NonPersistentTopic topic = new NonPersistentTopic(successTopicName, brokerService);
        // NonPersistentSubscription doesn't support transactions
        NonPersistentSubscription subscription = new NonPersistentSubscription(topic, successSubName, true);
        topic.getSubscriptions().put(successSubName, subscription);

        doReturn(CompletableFuture.completedFuture(Optional.of(topic)))
            .when(brokerService).getTopicIfExists(successTopicName);

        try {
            blockingStub.endTransactionOnSubscription(Commands.newEndTxnOnSubscription(
                    100, 200, successTopicName, successSubName, TxnAction.ABORT, 150));
            fail("StatusRuntimeException should have been thrown");
        } catch (StatusRuntimeException e) {
            assertErrorIsStatusExceptionWithServerError(e, Status.INTERNAL, ServerError.UnknownError);
        }
    }

    @Test
    public void testProduceSingle() {
        CommandProducer producerParams = Commands.newProducer(successTopicName, "prod-name", Collections.emptyMap());

        MessageMetadata messageMetadata = new MessageMetadata()
                .setPublishTime(System.currentTimeMillis())
                .setProducerName("prod-name")
                .setSequenceId(0);

        CommandSend send = Commands.newSend(1, 0, 1,
                messageMetadata, Unpooled.buffer(1024));

        CommandProduceSingle produceSingle = CommandProduceSingle.newBuilder()
                .setProducer(producerParams)
                .setSend(send)
                .build();

        CommandSendReceipt sendReceipt = blockingStub.produceSingle(produceSingle);
        assertEquals(sendReceipt.getSequenceId(), 1);
    }

    @Test
    public void testProduceSingleError() {
        String invalidTopicName = "xx/ass/aa/aaa";
        CommandProducer producerParams = Commands.newProducer(invalidTopicName, "prod-name", Collections.emptyMap());

        MessageMetadata messageMetadata = new MessageMetadata()
                .setPublishTime(System.currentTimeMillis())
                .setProducerName("prod-name")
                .setSequenceId(0);

        CommandSend send = Commands.newSend(1, 0, 1,
                messageMetadata, Unpooled.buffer(1024));

        CommandProduceSingle produceSingle = CommandProduceSingle.newBuilder()
                .setProducer(producerParams)
                .setSend(send)
                .build();

        try {
            blockingStub.produceSingle(produceSingle);
            fail("StatusRuntimeException should have been sent");
        } catch (StatusRuntimeException e) {
            assertErrorIsStatusExceptionWithServerError(e, Status.INVALID_ARGUMENT, ServerError.InvalidTopicName);
        }
    }

    @Test
    public void testProduceSingleSendError() throws Exception {
        // Set encryption_required to true
        ZooKeeperDataCache<Policies> zkDataCache = mock(ZooKeeperDataCache.class);
        Policies policies = mock(Policies.class);
        policies.encryption_required = true;
        policies.topicDispatchRate = Maps.newHashMap();
        policies.clusterDispatchRate = Maps.newHashMap();
        doReturn(Optional.of(policies)).when(zkDataCache)
                .get(AdminResource.path(POLICIES, TopicName.get(encryptionRequiredTopicName).getNamespace()));
        doReturn(CompletableFuture.completedFuture(Optional.of(policies))).when(zkDataCache)
                .getAsync(AdminResource.path(POLICIES, TopicName.get(encryptionRequiredTopicName).getNamespace()));
        doReturn(zkDataCache).when(configCacheService).policiesCache();

        CommandProducer producerParams = Commands.newProducer(encryptionRequiredTopicName,
                "prod-name", true, null);

        MessageMetadata messageMetadata = new MessageMetadata()
                .setPublishTime(System.currentTimeMillis())
                .setProducerName("prod-name")
                .setSequenceId(0);

        CommandSend send = Commands.newSend(1, 0, 1,
                messageMetadata, Unpooled.buffer(1024));

        CommandProduceSingle produceSingle = CommandProduceSingle.newBuilder()
                .setProducer(producerParams)
                .setSend(send)
                .build();

        try {
            blockingStub.produceSingle(produceSingle);
            fail("StatusRuntimeException should have been sent");
        } catch (StatusRuntimeException e) {
            assertErrorIsStatusExceptionWithServerError(e, Status.FAILED_PRECONDITION, ServerError.MetadataError);
        }
    }

    private void verifyProduceFails(CommandProducer producerParams, Status expectedStatus, ServerError expectedCode)
            throws ExecutionException, InterruptedException, TimeoutException {
        PulsarGrpc.PulsarStub producerStub = Commands.attachProducerParams(stub, producerParams);
        TestStreamObserver<SendResult> sendResult = new TestStreamObserver<>();
        producerStub.produce(sendResult);
        assertErrorIsStatusExceptionWithServerError(sendResult.waitForError(), expectedStatus, expectedCode);
    }

    private void verifyConsumeFails(CommandSubscribe consumerParams, Status expectedStatus, ServerError expectedCode)
            throws ExecutionException, InterruptedException, TimeoutException {
        PulsarGrpc.PulsarStub producerStub = Commands.attachConsumerParams(stub, consumerParams);
        TestStreamObserver<ConsumeOutput> sendResult = new TestStreamObserver<>();
        producerStub.consume(sendResult);
        assertErrorIsStatusExceptionWithServerError(sendResult.waitForError(), expectedStatus, expectedCode);
    }

    private static class TestStreamObserver<T> implements StreamObserver<T> {

        public static final int TIMEOUT = 10;

        public static <T> TestStreamObserver<T> create() {
            return new TestStreamObserver<T>();
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

        public T takeOneMessage() throws InterruptedException, TimeoutException {
            T poll = queue.poll(TIMEOUT, TimeUnit.SECONDS);
            if (poll == null) {
                throw new TimeoutException("Timeout occurred while waiting message");
            }
            return poll;
        }

        public T pollOneMessage() {
            return queue.poll();
        }

        public Throwable waitForError() throws ExecutionException, InterruptedException, TimeoutException {
            return error.get(TIMEOUT, TimeUnit.SECONDS);
        }

        public void waitForCompletion() throws InterruptedException {
            complete.await(TIMEOUT, TimeUnit.SECONDS);
        }

        public void waitForErrorOrCompletion() throws InterruptedException {
            errorOrComplete.await(TIMEOUT, TimeUnit.SECONDS);
        }
    }

    private static void assertErrorIsStatusExceptionWithServerError(Throwable actualException, Status expectedStatus,
            ServerError expectedCode) {
        Status actualStatus = Status.fromThrowable(actualException);
        assertEquals(actualStatus.getCode(), expectedStatus.getCode());

        Metadata actualMetadata = Status.trailersFromThrowable(actualException);
        assertNotNull(actualMetadata);
        assertEquals(ServerError.forNumber(Integer.parseInt(actualMetadata.get(ERROR_CODE_METADATA_KEY))),
                expectedCode);
    }

    @SuppressWarnings("unchecked")
    private void setupMLAsyncCallbackMocks() {
        doReturn(new ArrayList<Object>()).when(ledgerMock).getCursors();

        // call openLedgerComplete with ledgerMock on ML factory asyncOpen
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Thread.sleep(300);
                ((AsyncCallbacks.OpenLedgerCallback) invocationOnMock.getArguments()[2])
                        .openLedgerComplete(ledgerMock, null);
                return null;
            }
        }).when(mlFactoryMock).asyncOpen(matches(".*success.*"), any(ManagedLedgerConfig.class),
                any(AsyncCallbacks.OpenLedgerCallback.class), any(Supplier.class), any());

        // call openLedgerFailed on ML factory asyncOpen
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Thread.sleep(300);
                new Thread(() -> {
                    ((AsyncCallbacks.OpenLedgerCallback) invocationOnMock.getArguments()[2])
                            .openLedgerFailed(new ManagedLedgerException("Managed ledger failure"), null);
                }).start();

                return null;
            }
        }).when(mlFactoryMock).asyncOpen(matches(".*fail.*"), any(ManagedLedgerConfig.class),
                any(AsyncCallbacks.OpenLedgerCallback.class), any(Supplier.class), any());

        // call addComplete on ledger asyncAddEntry
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((AsyncCallbacks.AddEntryCallback) invocationOnMock.getArguments()[1])
                        .addComplete(new PositionImpl(-1, -1),
                                null,
                                invocationOnMock.getArguments()[2]);
                return null;
            }
        }).when(ledgerMock).asyncAddEntry(any(ByteBuf.class), any(AsyncCallbacks.AddEntryCallback.class), any());

        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Thread.sleep(300);
                ((AsyncCallbacks.OpenCursorCallback) invocationOnMock.getArguments()[2])
                        .openCursorComplete(cursorMock, null);
                return null;
            }
        }).when(ledgerMock)
                .asyncOpenCursor(matches(".*success.*"),
                        any(org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition.class),
                        any(AsyncCallbacks.OpenCursorCallback.class), any());

        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Thread.sleep(300);
                ((AsyncCallbacks.OpenCursorCallback) invocationOnMock.getArguments()[3])
                        .openCursorComplete(cursorMock, null);
                return null;
            }
        }).when(ledgerMock)
                .asyncOpenCursor(matches(".*success.*"),
                        any(org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition.class),
                        any(Map.class),
                        any(AsyncCallbacks.OpenCursorCallback.class), any());

        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Thread.sleep(300);
                ((AsyncCallbacks.OpenCursorCallback) invocationOnMock.getArguments()[2])
                        .openCursorFailed(new ManagedLedgerException("Managed ledger failure"), null);
                return null;
            }
        }).when(ledgerMock).asyncOpenCursor(matches(".*fail.*"), any(
                org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition.class),
                any(AsyncCallbacks.OpenCursorCallback.class), any());

        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Thread.sleep(300);
                ((AsyncCallbacks.OpenCursorCallback) invocationOnMock.getArguments()[3])
                        .openCursorFailed(new ManagedLedgerException("Managed ledger failure"), null);
                return null;
            }
        }).when(ledgerMock).asyncOpenCursor(matches(".*fail.*"),
                any(org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition.class),
                any(Map.class),
                any(AsyncCallbacks.OpenCursorCallback.class), any());

        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((AsyncCallbacks.DeleteCursorCallback) invocationOnMock.getArguments()[1]).deleteCursorComplete(null);
                return null;
            }
        }).when(ledgerMock)
                .asyncDeleteCursor(matches(".*success.*"), any(AsyncCallbacks.DeleteCursorCallback.class), any());

        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((AsyncCallbacks.DeleteCursorCallback) invocationOnMock.getArguments()[1])
                        .deleteCursorFailed(new ManagedLedgerException("Managed ledger failure"), null);
                return null;
            }
        }).when(ledgerMock)
                .asyncDeleteCursor(matches(".*fail.*"), any(AsyncCallbacks.DeleteCursorCallback.class), any());

        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((AsyncCallbacks.CloseCallback) invocationOnMock.getArguments()[0]).closeComplete(null);
                return null;
            }
        }).when(cursorMock).asyncClose(any(AsyncCallbacks.CloseCallback.class), any());

        doReturn(successSubName).when(cursorMock).getName();
        doReturn(true).when(cursorMock).isDurable();
    }
}
