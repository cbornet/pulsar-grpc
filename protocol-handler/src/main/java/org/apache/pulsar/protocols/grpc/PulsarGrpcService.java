/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.protocols.grpc;

import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.netty.channel.EventLoopGroup;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataCommand;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.service.schema.SchemaRegistryService;
import org.apache.pulsar.broker.service.schema.exceptions.IncompatibleSchemaException;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.naming.Metadata;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.protocol.schema.SchemaInfoUtil;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.SafeCollectionUtils;
import org.apache.pulsar.protocols.grpc.api.CommandAddPartitionToTxn;
import org.apache.pulsar.protocols.grpc.api.CommandAddPartitionToTxnResponse;
import org.apache.pulsar.protocols.grpc.api.CommandConsumerStats;
import org.apache.pulsar.protocols.grpc.api.CommandEndTxn;
import org.apache.pulsar.protocols.grpc.api.CommandEndTxnResponse;
import org.apache.pulsar.protocols.grpc.api.CommandFlow;
import org.apache.pulsar.protocols.grpc.api.CommandGetLastMessageId;
import org.apache.pulsar.protocols.grpc.api.CommandGetOrCreateSchema;
import org.apache.pulsar.protocols.grpc.api.CommandGetOrCreateSchemaResponse;
import org.apache.pulsar.protocols.grpc.api.CommandGetSchema;
import org.apache.pulsar.protocols.grpc.api.CommandGetSchemaResponse;
import org.apache.pulsar.protocols.grpc.api.CommandGetTopicsOfNamespace;
import org.apache.pulsar.protocols.grpc.api.CommandGetTopicsOfNamespaceResponse;
import org.apache.pulsar.protocols.grpc.api.CommandLookupTopic;
import org.apache.pulsar.protocols.grpc.api.CommandLookupTopicResponse;
import org.apache.pulsar.protocols.grpc.api.CommandNewTxn;
import org.apache.pulsar.protocols.grpc.api.CommandNewTxnResponse;
import org.apache.pulsar.protocols.grpc.api.CommandPartitionedTopicMetadata;
import org.apache.pulsar.protocols.grpc.api.CommandPartitionedTopicMetadataResponse;
import org.apache.pulsar.protocols.grpc.api.CommandProduceSingle;
import org.apache.pulsar.protocols.grpc.api.CommandProducer;
import org.apache.pulsar.protocols.grpc.api.CommandRedeliverUnacknowledgedMessages;
import org.apache.pulsar.protocols.grpc.api.CommandSeek;
import org.apache.pulsar.protocols.grpc.api.CommandSend;
import org.apache.pulsar.protocols.grpc.api.CommandSendError;
import org.apache.pulsar.protocols.grpc.api.CommandSendReceipt;
import org.apache.pulsar.protocols.grpc.api.CommandSubscribe;
import org.apache.pulsar.protocols.grpc.api.CommandSubscribe.InitialPosition;
import org.apache.pulsar.protocols.grpc.api.CommandSubscribe.SubType;
import org.apache.pulsar.protocols.grpc.api.CommandUnsubscribe;
import org.apache.pulsar.protocols.grpc.api.ConsumeInput;
import org.apache.pulsar.protocols.grpc.api.ConsumeOutput;
import org.apache.pulsar.protocols.grpc.api.KeySharedMeta;
import org.apache.pulsar.protocols.grpc.api.MessageIdData;
import org.apache.pulsar.protocols.grpc.api.PulsarGrpc;
import org.apache.pulsar.protocols.grpc.api.Schema;
import org.apache.pulsar.protocols.grpc.api.SendResult;
import org.apache.pulsar.protocols.grpc.api.ServerError;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.impl.common.TxnStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.pulsar.broker.admin.impl.PersistentTopicsBase.unsafeGetPartitionedTopicMetadataAsync;
import static org.apache.pulsar.common.protocol.Commands.parseMessageMetadata;
import static org.apache.pulsar.protocols.grpc.Commands.convertCommandAck;
import static org.apache.pulsar.protocols.grpc.Commands.convertGetTopicsOfNamespaceMode;
import static org.apache.pulsar.protocols.grpc.Commands.convertKeySharedMeta;
import static org.apache.pulsar.protocols.grpc.Commands.convertServerError;
import static org.apache.pulsar.protocols.grpc.Commands.convertSubscribeInitialPosition;
import static org.apache.pulsar.protocols.grpc.Commands.convertSubscribeSubType;
import static org.apache.pulsar.protocols.grpc.Commands.newStatusException;
import static org.apache.pulsar.protocols.grpc.Constants.AUTH_DATA_CTX_KEY;
import static org.apache.pulsar.protocols.grpc.Constants.AUTH_ROLE_CTX_KEY;
import static org.apache.pulsar.protocols.grpc.Constants.CONSUMER_PARAMS_CTX_KEY;
import static org.apache.pulsar.protocols.grpc.Constants.PRODUCER_PARAMS_CTX_KEY;
import static org.apache.pulsar.protocols.grpc.Constants.REMOTE_ADDRESS_CTX_KEY;
import static org.apache.pulsar.protocols.grpc.TopicLookup.lookupTopicAsync;

public class PulsarGrpcService extends PulsarGrpc.PulsarImplBase {

    private static final Logger log = LoggerFactory.getLogger(PulsarGrpcService.class);

    private final BrokerService service;
    private final SchemaRegistryService schemaService;
    private final EventLoopGroup eventLoopGroup;
    private final ServiceConfiguration configuration;

    public PulsarGrpcService(BrokerService service, ServiceConfiguration configuration, EventLoopGroup eventLoopGroup) {
        this.service = service;
        this.schemaService = service.pulsar().getSchemaRegistryService();
        this.eventLoopGroup = eventLoopGroup;
        this.configuration = configuration;
    }

    private CompletableFuture<Boolean> isTopicOperationAllowed(TopicName topicName, TopicOperation operation,
            String authRole, AuthenticationDataSource authenticationData) {
        CompletableFuture<Boolean> isAuthorizedFuture;
        if (service.isAuthorizationEnabled()) {
            isAuthorizedFuture = service.getAuthorizationService().allowTopicOperationAsync(
                    topicName, operation, authRole, authenticationData);
        } else {
            isAuthorizedFuture = CompletableFuture.completedFuture(true);
        }
        return isAuthorizedFuture.thenApply(isAuthorized -> {
            if (!isAuthorized) {
                log.warn("Role {} is not authorized to perform operation {} on topic {}",
                        authRole, operation, topicName);
            }
            return isAuthorized;
        });
    }

    private CompletableFuture<Boolean> isTopicOperationAllowed(TopicName topicName, String subscriptionName,
            TopicOperation operation, String authRole, AuthenticationDataSource authenticationData) {
        CompletableFuture<Boolean> isAuthorizedFuture;
        if (service.isAuthorizationEnabled()) {
            if (authenticationData == null) {
                authenticationData = new AuthenticationDataCommand("", subscriptionName);
            } else {
                authenticationData.setSubscription(subscriptionName);
            }
            return isTopicOperationAllowed(topicName, operation, authRole, authenticationData);
        } else {
            isAuthorizedFuture = CompletableFuture.completedFuture(true);
        }
        return isAuthorizedFuture.thenApply(isAuthorized -> {
            if (!isAuthorized) {
                log.warn("Role {} is not authorized to perform operation {} on topic {}, subscription {}",
                        authRole, operation, topicName, subscriptionName);
            }
            return isAuthorized;
        });
    }



    @Override
    public void lookupTopic(CommandLookupTopic lookup, StreamObserver<CommandLookupTopicResponse> responseObserver) {
        final SocketAddress remoteAddress = REMOTE_ADDRESS_CTX_KEY.get();
        final String authRole = AUTH_ROLE_CTX_KEY.get();
        final AuthenticationDataSource authenticationData = AUTH_DATA_CTX_KEY.get();
        final boolean authoritative = lookup.getAuthoritative();
        final String advertisedListenerName = lookup.getAdvertisedListenerName();

        if (log.isDebugEnabled()) {
            log.debug("[{}] Received Lookup from {}", lookup.getTopic(), remoteAddress);
        }

        TopicName topicName;
        try {
            topicName = TopicName.get(lookup.getTopic());
        } catch (Throwable t) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Failed to parse topic name '{}'", remoteAddress, lookup.getTopic(), t);
            }
            responseObserver.onError(newStatusException(Status.INVALID_ARGUMENT,
                    "Invalid topic name: " + t.getMessage(), null, ServerError.InvalidTopicName));
            return;
        }

        final Semaphore lookupSemaphore = service.getLookupRequestSemaphore();
        if (lookupSemaphore.tryAcquire()) {
            isTopicOperationAllowed(topicName, TopicOperation.LOOKUP, authRole, authenticationData).thenApply(isAuthorized -> {
                if (isAuthorized) {
                    lookupTopicAsync(service.pulsar(), topicName, authoritative, authRole, authenticationData, advertisedListenerName)
                            .handle((lookupResponse, ex) -> {
                                if (ex == null) {
                                    responseObserver.onNext(lookupResponse);
                                    responseObserver.onCompleted();
                                } else {
                                    responseObserver.onError(ex);
                                }
                                lookupSemaphore.release();
                                return null;
                            });
                } else {
                    final String msg = "Client is not authorized to Lookup";
                    log.warn("[{}] {} with role {} on topic {}", remoteAddress, msg, authRole, topicName);
                    responseObserver.onError(newStatusException(Status.PERMISSION_DENIED, msg, null, ServerError.AuthorizationError));
                    lookupSemaphore.release();
                }
                return null;
            }).exceptionally(ex -> {
                final String msg = "Exception occured while trying to authorize lookup";
                log.warn("[{}] {} with role {} on topic {}", remoteAddress, msg, authRole, topicName, ex);
                responseObserver.onError(newStatusException(Status.PERMISSION_DENIED, msg, ex, ServerError.AuthorizationError));
                lookupSemaphore.release();
                return null;
            });
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Failed lookup due to too many lookup-requests {}", remoteAddress, topicName);
            }
            responseObserver.onError(newStatusException(Status.RESOURCE_EXHAUSTED,
                    "Failed due to too many pending lookup requests", null, ServerError.TooManyRequests));
        }
    }

    @Override
    public void getSchema(CommandGetSchema commandGetSchema, StreamObserver<CommandGetSchemaResponse> responseObserver) {
        SocketAddress remoteAddress = REMOTE_ADDRESS_CTX_KEY.get();
        if (log.isDebugEnabled()) {
            log.debug("Received CommandGetSchema call from {}, schemaVersion: {}, topic: {}",
                    remoteAddress, new String(commandGetSchema.getSchemaVersion().toByteArray()),
                    commandGetSchema.getTopic());
        }

        SchemaVersion schemaVersion = SchemaVersion.Latest;
        if (commandGetSchema.hasSchemaVersion()) {
            schemaVersion = schemaService.versionFromBytes(commandGetSchema.getSchemaVersion().toByteArray());
        }

        String schemaName;
        try {
            schemaName = TopicName.get(commandGetSchema.getTopic()).getSchemaName();
        } catch (Throwable t) {
            responseObserver.onError(newStatusException(Status.INVALID_ARGUMENT, t, ServerError.InvalidTopicName));
            return;
        }

        schemaService.getSchema(schemaName, schemaVersion).thenAccept(schemaAndMetadata -> {
            if (schemaAndMetadata == null) {
                responseObserver.onError(newStatusException(Status.INVALID_ARGUMENT, "Topic not found or no-schema",
                        null, ServerError.TopicNotFound));
            } else {
                responseObserver.onNext(Commands.newGetSchemaResponse(
                        SchemaInfoUtil.newSchemaInfo(schemaName, schemaAndMetadata.schema), schemaAndMetadata.version));
                responseObserver.onCompleted();
            }
        }).exceptionally(ex -> {
            responseObserver.onError(newStatusException(Status.INTERNAL, ex, ServerError.UnknownError));
            return null;
        });
    }

    @Override
    public void getOrCreateSchema(CommandGetOrCreateSchema commandGetOrCreateSchema, StreamObserver<CommandGetOrCreateSchemaResponse> responseObserver) {
        SocketAddress remoteAddress = REMOTE_ADDRESS_CTX_KEY.get();
        if (log.isDebugEnabled()) {
            log.debug("Received CommandGetOrCreateSchema call from {}", remoteAddress);
        }
        String topicName = commandGetOrCreateSchema.getTopic();
        SchemaData schemaData = getSchema(commandGetOrCreateSchema.getSchema());
        SchemaData schema = schemaData.getType() == SchemaType.NONE ? null : schemaData;
        service.getTopicIfExists(topicName).thenAccept(topicOpt -> {
            if (topicOpt.isPresent()) {
                Topic topic = topicOpt.get();
                CompletableFuture<SchemaVersion> schemaVersionFuture = tryAddSchema(topic, schema, remoteAddress);
                schemaVersionFuture.exceptionally(ex -> {
                    ServerError errorCode = convertServerError(BrokerServiceException.getClientErrorCode(ex));
                    responseObserver.onError(Commands.newStatusException(Status.INTERNAL, ex, errorCode));
                    return null;
                }).thenAccept(schemaVersion -> {
                    responseObserver.onNext(Commands.newGetOrCreateSchemaResponse(schemaVersion));
                    responseObserver.onCompleted();
                });
            } else {
                responseObserver.onError(Commands.newStatusException(Status.INVALID_ARGUMENT, "Topic not found", null,
                        ServerError.TopicNotFound));
            }
        }).exceptionally(ex -> {
            ServerError errorCode = convertServerError(BrokerServiceException.getClientErrorCode(ex));
            responseObserver.onError(Commands.newStatusException(Status.INTERNAL, ex, errorCode));
            return null;
        });
    }

    @Override
    public void getPartitionMetadata(CommandPartitionedTopicMetadata partitionMetadata,
            StreamObserver<CommandPartitionedTopicMetadataResponse> responseObserver) {
        final SocketAddress remoteAddress = REMOTE_ADDRESS_CTX_KEY.get();
        final String authRole = AUTH_ROLE_CTX_KEY.get();
        final AuthenticationDataSource authenticationData = AUTH_DATA_CTX_KEY.get();

        if (log.isDebugEnabled()) {
            log.debug("[{}] Received PartitionMetadataLookup from {}", partitionMetadata.getTopic(),
                    remoteAddress);
        }

        TopicName topicName;
        try {
            topicName = TopicName.get(partitionMetadata.getTopic());
        } catch (Throwable t) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Failed to parse topic name '{}'", remoteAddress, partitionMetadata.getTopic(), t);
            }
            responseObserver.onError(newStatusException(Status.INVALID_ARGUMENT,
                    "Invalid topic name: " + t.getMessage(), null, ServerError.InvalidTopicName));
            return;
        }

        final Semaphore lookupSemaphore = service.getLookupRequestSemaphore();
        if (lookupSemaphore.tryAcquire()) {
            isTopicOperationAllowed(topicName, TopicOperation.LOOKUP, authRole, authenticationData).thenApply(isAuthorized -> {
                if (isAuthorized) {
                    unsafeGetPartitionedTopicMetadataAsync(service.pulsar(), topicName)
                            .handle((metadata, ex) -> {
                                if (ex == null) {
                                    int partitions = metadata.partitions;
                                    responseObserver.onNext(Commands.newPartitionMetadataResponse(partitions));
                                    responseObserver.onCompleted();
                                } else {
                                    if (ex instanceof PulsarClientException) {
                                        log.warn("Failed to authorize {} at [{}] on topic {} : {}", authRole,
                                                remoteAddress, topicName, ex.getMessage());
                                        responseObserver.onError(Commands.newStatusException(Status.PERMISSION_DENIED, ex,
                                                ServerError.AuthorizationError));
                                    } else {
                                        log.warn("Failed to get Partitioned Metadata [{}] {}: {}", remoteAddress,
                                                topicName, ex.getMessage(), ex);
                                        ServerError error = (ex instanceof RestException)
                                                && ((RestException) ex).getResponse().getStatus() < 500
                                                ? ServerError.MetadataError
                                                : ServerError.ServiceNotReady;
                                        responseObserver.onError(Commands.newStatusException(Status.FAILED_PRECONDITION, ex, error));
                                    }
                                }
                                lookupSemaphore.release();
                                return null;
                            });
                } else {
                    final String msg = "Client is not authorized to Get Partition Metadata";
                    log.warn("[{}] {} with role {} on topic {}", remoteAddress, msg, authRole, topicName);
                    responseObserver.onError(newStatusException(Status.PERMISSION_DENIED, msg, null, ServerError.AuthorizationError));
                    lookupSemaphore.release();
                }
                return null;
            }).exceptionally(ex -> {
                final String msg = "Exception occured while trying to authorize get Partition Metadata";
                log.warn("[{}] {} with role {} on topic {}", remoteAddress, msg, authRole, topicName);
                responseObserver.onError(newStatusException(Status.PERMISSION_DENIED, msg, ex, ServerError.AuthorizationError));
                lookupSemaphore.release();
                return null;
            });
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Failed Partition-Metadata lookup due to too many lookup-requests {}", remoteAddress,
                        topicName);
            }
            responseObserver.onError(Commands.newStatusException(Status.RESOURCE_EXHAUSTED,
                    "Failed due to too many pending lookup requests", null, ServerError.TooManyRequests));
        }
    }

    @Override
    public void getTopicsOfNamespace(CommandGetTopicsOfNamespace commandGetTopicsOfNamespace,
            StreamObserver<CommandGetTopicsOfNamespaceResponse> responseObserver) {
        final SocketAddress remoteAddress = REMOTE_ADDRESS_CTX_KEY.get();
        final String namespace = commandGetTopicsOfNamespace.getNamespace();
        final CommandGetTopicsOfNamespace.Mode mode = commandGetTopicsOfNamespace.getMode();
        final NamespaceName namespaceName = NamespaceName.get(namespace);

        service.pulsar().getNamespaceService().getListOfTopics(namespaceName, convertGetTopicsOfNamespaceMode(mode))
                .thenAccept(topics -> {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Received CommandGetTopicsOfNamespace for namespace [//{}], size:{}",
                                remoteAddress, namespace, topics.size());
                    }

                    responseObserver.onNext(Commands.newGetTopicsOfNamespaceResponse(topics));
                    responseObserver.onCompleted();
                })
                .exceptionally(ex -> {
                    log.warn("[{}] Error GetTopicsOfNamespace for namespace [//{}]",
                            remoteAddress, namespace);
                    responseObserver.onError(Commands.newStatusException(Status.INTERNAL, ex,
                            convertServerError(BrokerServiceException.getClientErrorCode(new BrokerServiceException.ServerMetadataException(ex)))));
                    return null;
                });
    }

    @Override
    public void createTransaction(CommandNewTxn command, StreamObserver<CommandNewTxnResponse> responseObserver) {
        SocketAddress remoteAddress = REMOTE_ADDRESS_CTX_KEY.get();
        if (log.isDebugEnabled()) {
            log.debug("Receive new txn request to transaction meta store {} from {}.", command.getTcId(), remoteAddress);
        }
        TransactionCoordinatorID tcId = TransactionCoordinatorID.get(command.getTcId());
        service.pulsar().getTransactionMetadataStoreService().newTransaction(tcId)
                .whenComplete(((txnID, ex) -> {
                    if (ex == null) {
                        if (log.isDebugEnabled()) {
                            log.debug("Send response {} for new txn request", tcId.getId());
                        }
                        responseObserver.onNext(Commands.newTxnResponse(txnID.getLeastSigBits(), txnID.getMostSigBits()));
                        responseObserver.onCompleted();
                    } else {
                        if (log.isDebugEnabled()) {
                            log.debug("Send response error for new txn request", ex);
                        }
                        responseObserver.onError(Commands.newStatusException(Status.FAILED_PRECONDITION, ex,
                                convertServerError(BrokerServiceException.getClientErrorCode(ex))));
                    }
                }));
    }

    @Override
    public void addPartitionsToTransaction(CommandAddPartitionToTxn command, StreamObserver<CommandAddPartitionToTxnResponse> responseObserver) {
        SocketAddress remoteAddress = REMOTE_ADDRESS_CTX_KEY.get();
        TxnID txnID = new TxnID(command.getTxnidMostBits(), command.getTxnidLeastBits());
        if (log.isDebugEnabled()) {
            log.debug("Receive add published partition to txn request from {} with txnId {}", remoteAddress, txnID);
        }
        service.pulsar().getTransactionMetadataStoreService().addProducedPartitionToTxn(txnID, command.getPartitionsList())
                .whenComplete(((v, ex) -> {
                    if (ex == null) {
                        if (log.isDebugEnabled()) {
                            log.debug("Send response success for add published partition to txn request");
                        }
                        responseObserver.onNext(Commands.newAddPartitionToTxnResponse());
                        responseObserver.onCompleted();
                    } else {
                        if (log.isDebugEnabled()) {
                            log.debug("Send response error for add published partition to txn request", ex);
                        }
                        responseObserver.onError(Commands.newStatusException(Status.FAILED_PRECONDITION, ex,
                                convertServerError(BrokerServiceException.getClientErrorCode(ex))));
                    }
                }));
    }

    @Override
    public void endTransaction(CommandEndTxn command, StreamObserver<CommandEndTxnResponse> responseObserver) {
        SocketAddress remoteAddress = REMOTE_ADDRESS_CTX_KEY.get();
        TxnStatus newStatus = null;
        switch (command.getTxnAction()) {
            case COMMIT:
                newStatus = TxnStatus.COMMITTING;
                break;
            case ABORT:
                newStatus = TxnStatus.ABORTING;
                break;
        }
        TxnID txnID = new TxnID(command.getTxnidMostBits(), command.getTxnidLeastBits());
        if (log.isDebugEnabled()) {
            log.debug("Receive end txn by {} request from {} with txnId {}", newStatus, remoteAddress, txnID);
        }
        service.pulsar().getTransactionMetadataStoreService().updateTxnStatus(txnID, newStatus, TxnStatus.OPEN)
                .whenComplete((v, ex) -> {
                    if (ex == null) {
                        if (log.isDebugEnabled()) {
                            log.debug("Send response success for end txn request");
                        }
                        responseObserver.onNext(Commands.newEndTxnResponse(txnID.getLeastSigBits(), txnID.getMostSigBits()));
                        responseObserver.onCompleted();
                    } else {
                        if (log.isDebugEnabled()) {
                            log.debug("Send response error for end txn request");
                        }
                        responseObserver.onError(Commands.newStatusException(Status.FAILED_PRECONDITION, ex,
                                convertServerError(BrokerServiceException.getClientErrorCode(ex))));
                    }
                });
    }

    @Override
    public void produceSingle(CommandProduceSingle request, StreamObserver<CommandSendReceipt> responseObserver) {
        Context ctx = Context.current().withValue(PRODUCER_PARAMS_CTX_KEY, request.getProducer());
        Context previousCtx = ctx.attach();
        AtomicReference<StreamObserver<CommandSend>> producer = new AtomicReference<>();
        StreamObserver<SendResult> produceObserver = new CallStreamObserver<SendResult>() {
            @Override
            public boolean isReady() {
                return true;
            }

            @Override
            public void setOnReadyHandler(Runnable onReadyHandler) {
                // Nothing to do
            }

            @Override
            public void disableAutoInboundFlowControl() {
                // Nothing to do
            }

            @Override
            public void request(int count) {
                // Nothing to do
            }

            @Override
            public void setMessageCompression(boolean enable) {
                // Nothing to do
            }

            @Override
            public void onNext(SendResult sendResult) {
                if (sendResult.hasSendReceipt()) {
                    responseObserver.onNext(sendResult.getSendReceipt());
                    producer.get().onCompleted();
                } else if (sendResult.hasSendError()) {
                    CommandSendError sendError = sendResult.getSendError();
                    responseObserver.onError(newStatusException(Status.FAILED_PRECONDITION, sendError.getMessage(),
                            null, sendError.getError()));
                    producer.get().onCompleted();
                } else if (sendResult.hasProducerSuccess()) {
                    producer.get().onNext(request.getSend());
                }
            }

            @Override
            public void onError(Throwable t) {
                responseObserver.onError(t);
            }

            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
            }
        };
        producer.set(produce(produceObserver));
        ctx.detach(previousCtx);
    }

    @Override
    public StreamObserver<CommandSend> produce(StreamObserver<SendResult> responseObserver) {
        final CommandProducer cmdProducer = PRODUCER_PARAMS_CTX_KEY.get();
        if (cmdProducer == null) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Missing CommandProducer header").asException());
            return NoOpStreamObserver.create();
        }

        final String authRole = AUTH_ROLE_CTX_KEY.get();
        final AuthenticationDataSource authenticationData = AUTH_DATA_CTX_KEY.get();
        final SocketAddress remoteAddress = REMOTE_ADDRESS_CTX_KEY.get();

        // Use producer name provided by client if present
        final String producerName = cmdProducer.hasProducerName() ? cmdProducer.getProducerName()
                : service.generateUniqueProducerName();
        final long epoch = cmdProducer.getEpoch();
        final boolean userProvidedProducerName = cmdProducer.getUserProvidedProducerName();
        final boolean isEncrypted = cmdProducer.getEncrypted();
        final Map<String, String> metadata = cmdProducer.getMetadataMap();
        final SchemaData schema = cmdProducer.hasSchema() ? getSchema(cmdProducer.getSchema()) : null;

        ProducerCnx cnx = new ProducerCnx(service, remoteAddress, authRole, authenticationData,
                responseObserver, eventLoopGroup.next());

        TopicName topicName;
        try {
            topicName = TopicName.get(cmdProducer.getTopic());
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Failed to parse topic name '{}'", remoteAddress, cmdProducer.getTopic(), e);
            }
            responseObserver.onError(newStatusException(Status.INVALID_ARGUMENT,
                    "Invalid topic name: " + e.getMessage(), e, ServerError.InvalidTopicName));
            return NoOpStreamObserver.create();
        }

        CompletableFuture<Boolean> isAuthorizedFuture = isTopicOperationAllowed(
                topicName, TopicOperation.PRODUCE, authRole, authenticationData
        );
        CompletableFuture<Producer> producerFuture = new CompletableFuture<>();
        isAuthorizedFuture.thenApply(isAuthorized -> {
            if (isAuthorized) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Client is authorized to Produce with role {}", remoteAddress, authRole);
                }
                service.getOrCreateTopic(topicName.toString()).thenAccept((Topic topic) -> {
                    // Before creating producer, check if backlog quota exceeded on topic
                    if (topic.isBacklogQuotaExceeded(producerName)) {
                        IllegalStateException illegalStateException = new IllegalStateException(
                                "Cannot create producer on topic with backlog quota exceeded");
                        BacklogQuota.RetentionPolicy retentionPolicy = topic.getBacklogQuota().getPolicy();
                        if (retentionPolicy == BacklogQuota.RetentionPolicy.producer_request_hold) {
                            responseObserver.onError(newStatusException(Status.FAILED_PRECONDITION,
                                    illegalStateException, ServerError.ProducerBlockedQuotaExceededError));
                        } else if (retentionPolicy == BacklogQuota.RetentionPolicy.producer_exception) {
                            responseObserver.onError(newStatusException(Status.FAILED_PRECONDITION,
                                    illegalStateException, ServerError.ProducerBlockedQuotaExceededException));
                        }
                        producerFuture.completeExceptionally(illegalStateException);
                        return;
                    }

                    // Check whether the producer will publish encrypted messages or not
                    if ((topic.isEncryptionRequired() || configuration.isEncryptionRequireOnProducer()) && !isEncrypted) {
                        String msg = String.format("Encryption is required in %s", topicName);
                        log.warn("[{}] {}", remoteAddress, msg);
                        responseObserver.onError(newStatusException(Status.INVALID_ARGUMENT, msg, null,
                                ServerError.MetadataError));
                        return;
                    }

                    CompletableFuture<SchemaVersion> schemaVersionFuture = tryAddSchema(topic, schema, remoteAddress);

                    schemaVersionFuture.exceptionally(exception -> {
                        responseObserver.onError(newStatusException(Status.FAILED_PRECONDITION, exception,
                                convertServerError(BrokerServiceException.getClientErrorCode(exception))));
                        return null;
                    });

                    schemaVersionFuture.thenAccept(schemaVersion -> {
                        Producer producer = new Producer(topic, cnx, 0L, producerName, authRole,
                                isEncrypted, metadata, schemaVersion, epoch, userProvidedProducerName);

                        try {
                            topic.addProducer(producer);
                            if (producerFuture.complete(producer)) {
                                log.info("[{}] Created new producer: {}", remoteAddress, producer);
                                responseObserver.onNext(Commands.newProducerSuccess(producerName,
                                        producer.getLastSequenceId(), producer.getSchemaVersion()));
                            } else {
                                // The producer's future was completed before by
                                // a close command
                                producer.closeNow(true);
                                log.info("[{}] Cleared producer created after timeout on client side {}",
                                        remoteAddress, producer);
                            }
                        } catch (Exception e) {
                            log.error("[{}] Failed to add producer to topic {}: {}", remoteAddress, topicName,
                                    e.getMessage());
                            responseObserver.onError(newStatusException(Status.FAILED_PRECONDITION, e,
                                    convertServerError(BrokerServiceException.getClientErrorCode(e))));
                            producerFuture.completeExceptionally(e);
                        }
                    });
                }).exceptionally(exception -> {
                    Throwable cause = exception.getCause();

                    if (cause instanceof NoSuchElementException) {
                        cause = new BrokerServiceException.TopicNotFoundException("Topic Not Found.");
                    }

                    if (!(cause instanceof BrokerServiceException.ServiceUnitNotReadyException)) {
                        // Do not print stack traces for expected exceptions
                        log.error("[{}] Failed to create topic {}", remoteAddress, topicName, exception);
                    }

                    if (producerFuture.completeExceptionally(exception)) {
                        responseObserver.onError(newStatusException(Status.FAILED_PRECONDITION, cause,
                                convertServerError(BrokerServiceException.getClientErrorCode(cause))));
                    }
                    return null;
                });
            } else {
                final String msg = "Client is not authorized to Produce";
                log.warn("[{}] {} with role {} on topic {}", remoteAddress, msg, authRole, topicName);
                responseObserver.onError(newStatusException(Status.PERMISSION_DENIED, msg, null,
                        ServerError.AuthorizationError));
            }
            return null;
        }).exceptionally(ex -> {
            String msg = String.format("[%s] %s with role %s", remoteAddress, ex.getMessage(), authRole);
            log.warn(msg);
            responseObserver.onError(newStatusException(Status.PERMISSION_DENIED, ex, ServerError.AuthorizationError));
            return null;
        });

        return new StreamObserver<CommandSend>() {
            @Override
            public void onNext(CommandSend cmd) {
                if (!producerFuture.isDone() || producerFuture.isCompletedExceptionally()) {
                    log.warn("[{}] Producer unavailable", remoteAddress);
                    return;
                }
                Producer producer = producerFuture.getNow(null);
                cnx.execute(() -> cnx.handleSend(cmd, producer));
            }

            @Override
            public void onError(Throwable throwable) {
                closeProduce(producerFuture, remoteAddress);
                responseObserver.onCompleted();
            }

            @Override
            public void onCompleted() {
                closeProduce(producerFuture, remoteAddress);
                responseObserver.onCompleted();
            }
        };
    }

    @Override
    public StreamObserver<ConsumeInput> consume(StreamObserver<ConsumeOutput> responseObserver) {
        final CommandSubscribe subscribe = CONSUMER_PARAMS_CTX_KEY.get();
        final String authRole = AUTH_ROLE_CTX_KEY.get();
        AuthenticationDataSource authenticationData = AUTH_DATA_CTX_KEY.get();
        final SocketAddress remoteAddress = REMOTE_ADDRESS_CTX_KEY.get();

        if (subscribe == null) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Missing CommandSubscribe header").asException());
            return NoOpStreamObserver.create();
        }

        TopicName topicName;
        try {
            topicName = TopicName.get(subscribe.getTopic());
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Failed to parse topic name '{}'", remoteAddress, subscribe.getTopic(), e);
            }
            responseObserver.onError(newStatusException(Status.INVALID_ARGUMENT,
                    "Invalid topic name: " + e.getMessage(), e, ServerError.InvalidTopicName));
            return NoOpStreamObserver.create();
        }

        final String subscriptionName = subscribe.getSubscription();
        final SubType subType = subscribe.getSubType();
        final String consumerName = subscribe.getConsumerName();
        final boolean isDurable = subscribe.getDurable();
        final MessageIdImpl startMessageId = subscribe.hasStartMessageId() ? new BatchMessageIdImpl(
                subscribe.getStartMessageId().getLedgerId(), subscribe.getStartMessageId().getEntryId(),
                subscribe.getStartMessageId().getPartition(), subscribe.getStartMessageId().getBatchIndex())
                : null;
        final int priorityLevel = subscribe.hasPriorityLevel() ? subscribe.getPriorityLevel() : 0;
        final boolean readCompacted = subscribe.getReadCompacted();
        final Map<String, String> metadata = subscribe.getMetadataMap();
        final InitialPosition initialPosition = subscribe.getInitialPosition();
        final long startMessageRollbackDurationSec = subscribe.hasStartMessageRollbackDurationSec()
                ? subscribe.getStartMessageRollbackDurationSec()
                : -1;
        final SchemaData schema = subscribe.hasSchema() ? getSchema(subscribe.getSchema()) : null;
        final boolean isReplicated = subscribe.hasReplicateSubscriptionState() && subscribe.getReplicateSubscriptionState();
        final boolean forceTopicCreation = subscribe.getForceTopicCreation();
        final KeySharedMeta keySharedMeta = subscribe.hasKeySharedMeta() ? subscribe.getKeySharedMeta() : null;

        CompletableFuture<Consumer> consumerFuture = new CompletableFuture<>();
        consumerFuture.thenAccept(consumer -> {
            //consumer.flowPermits(1);
        });

        CallStreamObserver<ConsumeOutput> consumerResponseObserver = (CallStreamObserver<ConsumeOutput>) responseObserver;
        consumerResponseObserver.disableAutoInboundFlowControl();

        class OnReadyHandler implements Runnable {
            // Guard against spurious onReady() calls caused by a race between onNext() and onReady(). If the transport
            // toggles isReady() from false to true while onNext() is executing, but before onNext() checks isReady(),
            // request(1) would be called twice - once by onNext() and once by the onReady() scheduled during onNext()'s
            // execution.
            private boolean wasReady = false;

            @Override
            public void run() {
                if (consumerResponseObserver.isReady() && !wasReady) {
                    wasReady = true;
                    consumerFuture.thenAccept(consumer -> {
                        consumer.flowPermits(1);
                    });
                    consumerResponseObserver.request(1);
                }
            }
        }

        final OnReadyHandler onReadyHandler = new OnReadyHandler();
        consumerResponseObserver.setOnReadyHandler(onReadyHandler);

        java.util.function.Consumer<Integer> cb = numMessages -> {
            if (consumerResponseObserver.isReady()) {
                consumerFuture.thenAccept(consumer -> {
                    consumer.flowPermits(numMessages);
                });
                consumerResponseObserver.request(numMessages);
            } else {
                // Back-pressure has begun.
                onReadyHandler.wasReady = false;
            }
        };

        ConsumerCnx cnx = new ConsumerCnx(service, remoteAddress, authRole, authenticationData, consumerResponseObserver,
                subscribe.getPreferedPayloadType(), cb);

        CompletableFuture<Boolean> isAuthorizedFuture = isTopicOperationAllowed(
                topicName,
                subscriptionName,
                TopicOperation.CONSUME,
                authRole,
                authenticationData
        );

        isAuthorizedFuture.thenApply(isAuthorized -> {
            if (isAuthorized) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Client is authorized to subscribe with role {}", remoteAddress, authRole);
                }

                log.info("[{}] Subscribing on topic {} / {}", remoteAddress, topicName, subscriptionName);
                try {
                    Metadata.validateMetadata(metadata);
                } catch (IllegalArgumentException iae) {
                    responseObserver.onError(
                            newStatusException(Status.INVALID_ARGUMENT, iae, ServerError.MetadataError));
                    return null;
                }

                boolean createTopicIfDoesNotExist = forceTopicCreation
                        && service.isAllowAutoTopicCreation(topicName.toString());

                service.getTopic(topicName.toString(), createTopicIfDoesNotExist)
                        .thenCompose(optTopic -> {
                            if (!optTopic.isPresent()) {
                                return FutureUtil
                                        .failedFuture(new BrokerServiceException.TopicNotFoundException("Topic does not exist"));
                            }

                            Topic topic = optTopic.get();

                            boolean rejectSubscriptionIfDoesNotExist = isDurable
                                    && !service.isAllowAutoSubscriptionCreation(topicName.toString())
                                    && !topic.getSubscriptions().containsKey(subscriptionName);

                            if (rejectSubscriptionIfDoesNotExist) {
                                return FutureUtil
                                        .failedFuture(new BrokerServiceException.SubscriptionNotFoundException("Subscription does not exist"));
                            }

                            if (schema != null) {
                                return topic.addSchemaIfIdleOrCheckCompatible(schema)
                                        .thenCompose(v -> topic.subscribe(cnx, subscriptionName, 0L,
                                                convertSubscribeSubType(subType), priorityLevel,
                                                consumerName, isDurable, startMessageId, metadata, readCompacted,
                                                convertSubscribeInitialPosition(initialPosition),
                                                startMessageRollbackDurationSec, isReplicated,
                                                convertKeySharedMeta(keySharedMeta)));
                            } else {
                                return topic.subscribe(cnx, subscriptionName, 0L,
                                        convertSubscribeSubType(subType), priorityLevel, consumerName, isDurable,
                                        startMessageId, metadata, readCompacted,
                                        convertSubscribeInitialPosition(initialPosition),
                                        startMessageRollbackDurationSec, isReplicated,
                                        convertKeySharedMeta(keySharedMeta));
                            }
                        })
                        .thenAccept(consumer -> {
                            if (consumerFuture.complete(consumer)) {
                                log.info("[{}] Created subscription on topic {} / {}", remoteAddress, topicName,
                                        subscriptionName);
                                responseObserver.onNext(Commands.newSubscriptionSuccess());
                            } else {
                                // The consumer future was completed before by a close command
                                try {
                                    consumer.close();
                                    log.info("[{}] Cleared consumer created after timeout on client side {}",
                                            remoteAddress, consumer);
                                } catch (BrokerServiceException e) {
                                    log.warn(
                                            "[{}] Error closing consumer created after timeout on client side {}: {}",
                                            remoteAddress, consumer, e.getMessage());
                                }
                            }

                        }) //
                        .exceptionally(exception -> {
                            if (exception.getCause() instanceof BrokerServiceException.ConsumerBusyException) {
                                if (log.isDebugEnabled()) {
                                    log.debug(
                                            "[{}][{}][{}] Failed to create consumer because exclusive consumer is already connected: {}",
                                            remoteAddress, topicName, subscriptionName,
                                            exception.getCause().getMessage());
                                }
                            } else if (exception.getCause() instanceof BrokerServiceException) {
                                log.warn("[{}][{}][{}] Failed to create consumer: {}", remoteAddress, topicName,
                                        subscriptionName, exception.getCause().getMessage());
                            } else {
                                log.warn("[{}][{}][{}] Failed to create consumer: {}", remoteAddress, topicName,
                                        subscriptionName, exception.getCause().getMessage(), exception);
                            }

                            responseObserver.onError(newStatusException(Status.FAILED_PRECONDITION, exception.getCause(),
                                    convertServerError(BrokerServiceException.getClientErrorCode(exception))));
                            consumerFuture.completeExceptionally(exception);
                            return null;
                        });
            } else {
                String msg = "Client is not authorized to subscribe";
                log.warn("[{}] {} with role {}", remoteAddress, msg, authRole);
                responseObserver.onError(newStatusException(Status.PERMISSION_DENIED, msg, null, ServerError.AuthorizationError));
            }
            return null;
        }).exceptionally(e -> {
            String msg = String.format("[%s] %s with role %s", remoteAddress, e.getMessage(), authRole);
            log.warn(msg);
            responseObserver.onError(newStatusException(Status.PERMISSION_DENIED, msg, e, ServerError.AuthorizationError));
            return null;
        });

        return new StreamObserver<ConsumeInput>() {
            @Override
            public void onNext(ConsumeInput consumeInput) {
                Consumer consumer = null;

                if (consumerFuture.isDone() && !consumerFuture.isCompletedExceptionally()) {
                    consumer = consumerFuture.getNow(null);
                }
                long requestId;
                switch (consumeInput.getConsumerInputOneofCase()) {
                    case ACK:
                        if (consumer != null) {
                            PulsarApi.CommandAck ack = convertCommandAck(consumeInput.getAck());
                            consumer.messageAcked(ack).thenRun(() -> {
                                if (ack.hasRequestId()) {
                                    responseObserver.onNext(Commands.newAckResponse(
                                            ack.getRequestId(), null, null));
                                }
                            }).exceptionally(e -> {
                                if (ack.hasRequestId()) {
                                    responseObserver.onNext(Commands.newAckResponse(ack.getRequestId(),
                                            convertServerError(BrokerServiceException.getClientErrorCode(e)),
                                            e.getMessage()));
                                }
                                return null;
                            });
                            ack.getMessageIdList().forEach(PulsarApi.MessageIdData::recycle);
                            ack.recycle();
                        }
                        break;
                    case FLOW:
                        CommandFlow flow = consumeInput.getFlow();
                        if (log.isDebugEnabled()) {
                            log.debug("[{}] Received flow permits: {}", remoteAddress, flow.getMessagePermits());
                        }

                        if (consumer != null) {
                            consumer.flowPermits(flow.getMessagePermits());
                        }
                        break;
                    case UNSUBSCRIBE:
                        CommandUnsubscribe unsubscribe = consumeInput.getUnsubscribe();
                        if (consumer != null) {
                            consumerFuture.getNow(null).doUnsubscribe(unsubscribe.getRequestId());
                        } else {
                            responseObserver.onNext(Commands.newError(unsubscribe.getRequestId(),
                                    ServerError.MetadataError, "Consumer not found"));
                        }
                        break;
                    case CONSUMERSTATS:
                        if (log.isDebugEnabled()) {
                            log.debug("Received CommandConsumerStats call from {}", remoteAddress);
                        }
                        CommandConsumerStats commandConsumerStats = consumeInput.getConsumerStats();
                        requestId = commandConsumerStats.getRequestId();

                        if (consumer == null) {
                            log.error(
                                    "Failed to get consumer-stats response - Consumer not found for CommandConsumerStats[remoteAddress = {}, requestId = {}]",
                                    remoteAddress, requestId);
                            responseObserver.onNext(Commands.newError(requestId, ServerError.ConsumerNotFound,
                                    "Consumer not found"));
                        } else {
                            if (log.isDebugEnabled()) {
                                log.debug("CommandConsumerStats[requestId = {}, consumer = {}]", requestId, consumer);
                            }
                            responseObserver.onNext(Commands.newConsumerStatsResponse(requestId, consumer.getStats(),
                                    consumer.getSubscription()));
                        }
                        break;
                    case REDELIVERUNACKNOWLEDGEDMESSAGES:
                        CommandRedeliverUnacknowledgedMessages redeliver = consumeInput.getRedeliverUnacknowledgedMessages();
                        if (log.isDebugEnabled()) {
                            log.debug("[{}] Received Resend Command from consumer", remoteAddress);
                        }
                        if (consumer != null) {
                            if (redeliver.getMessageIdsCount() > 0 && Subscription.isIndividualAckMode(consumer.subType())) {
                                List<PulsarApi.MessageIdData> messageIdDataList = redeliver.getMessageIdsList().stream()
                                        .map(Commands::convertMessageIdData)
                                        .collect(Collectors.toList());
                                consumer.redeliverUnacknowledgedMessages(messageIdDataList);
                                messageIdDataList.forEach(PulsarApi.MessageIdData::recycle);
                            } else {
                                consumer.redeliverUnacknowledgedMessages();
                            }
                        }
                        break;
                    case GETLASTMESSAGEID:
                        if (consumer != null) {
                            CommandGetLastMessageId getLastMessageId = consumeInput.getGetLastMessageId();

                            Topic topic = consumer.getSubscription().getTopic();
                            Position position = topic.getLastPosition();
                            int partitionIndex = TopicName.getPartitionIndex(topic.getName());

                            getLargestBatchIndexWhenPossible(
                                    topic,
                                    (PositionImpl) position,
                                    partitionIndex,
                                    getLastMessageId.getRequestId(),
                                    consumer.getSubscription().getName(),
                                    responseObserver,
                                    remoteAddress);
                        }
                        break;
                    case SEEK:
                        CommandSeek seek = consumeInput.getSeek();
                        requestId = seek.getRequestId();

                        if (consumer == null) {
                            responseObserver.onNext(Commands.newError(requestId, ServerError.MetadataError, "Consumer not found"));
                            return;
                        }
                        if (!seek.hasMessageId() && !seek.hasMessagePublishTime()) {
                            responseObserver.onNext(
                                    Commands.newError(requestId, ServerError.MetadataError,
                                            "Message id and message publish time were not present"));
                            return;
                        }
                        if (seek.hasMessageId()) {
                            Subscription subscription = consumer.getSubscription();
                            MessageIdData msgIdData = seek.getMessageId();

                            long[] ackSet = null;
                            if (msgIdData.getAckSetCount() > 0) {
                                ackSet = SafeCollectionUtils.longListToArray(msgIdData.getAckSetList());
                            }

                            Position position = new PositionImpl(msgIdData.getLedgerId(),
                                    msgIdData.getEntryId(), ackSet);

                            subscription.resetCursor(position).thenRun(() -> {
                                log.info("[{}] [{}][{}] Reset subscription to message id {}", remoteAddress,
                                        subscription.getTopic().getName(), subscription.getName(), position);
                                // At the moment the consumer is disconnected during a seek.
                                // See https://github.com/apache/pulsar/issues/5073
                                // responseObserver.onNext(Commands.newSuccess(requestId));
                            }).exceptionally(ex -> {
                                log.warn("[{}][{}] Failed to reset subscription: {}", remoteAddress, subscription, ex.getMessage(), ex);
                                responseObserver.onNext(Commands.newError(requestId, ServerError.UnknownError,
                                        "Error when resetting subscription: " + ex.getCause().getMessage()));
                                return null;
                            });
                        } else {
                            Subscription subscription = consumer.getSubscription();
                            long timestamp = seek.getMessagePublishTime();

                            subscription.resetCursor(timestamp).thenRun(() -> {
                                log.info("[{}] [{}][{}] Reset subscription to publish time {}", remoteAddress,
                                        subscription.getTopic().getName(), subscription.getName(), timestamp);
                                responseObserver.onNext(Commands.newSuccess(requestId));
                            }).exceptionally(ex -> {
                                log.warn("[{}][{}] Failed to reset subscription: {}", remoteAddress, subscription, ex.getMessage(), ex);
                                responseObserver.onNext(Commands.newError(requestId, ServerError.UnknownError,
                                        "Reset subscription to publish time error: " + ex.getCause().getMessage()));
                                return null;
                            });
                        }
                        break;

                    default:
                        break;
                }
            }

            @Override
            public void onError(Throwable throwable) {
                closeConsume(consumerFuture, remoteAddress, responseObserver);
            }

            @Override
            public void onCompleted() {
                closeConsume(consumerFuture, remoteAddress, responseObserver);
            }
        };
    }

    private void getLargestBatchIndexWhenPossible(
            Topic topic,
            PositionImpl position,
            int partitionIndex,
            long requestId,
            String subscriptionName,
            StreamObserver<ConsumeOutput> responseObserver,
            SocketAddress remoteAddress) {

        PersistentTopic persistentTopic = (PersistentTopic) topic;
        ManagedLedgerImpl ml = (ManagedLedgerImpl) persistentTopic.getManagedLedger();

        // If it's not pointing to a valid entry, respond messageId of the current position.
        if (position.getEntryId() == -1) {
            MessageIdData messageId = MessageIdData.newBuilder()
                    .setLedgerId(position.getLedgerId())
                    .setEntryId(position.getEntryId())
                    .setPartition(partitionIndex).build();

            responseObserver.onNext(Commands.newGetLastMessageIdResponse(requestId, messageId));
            return;
        }

        // For a valid position, we read the entry out and parse the batch size from its metadata.
        CompletableFuture<Entry> entryFuture = new CompletableFuture<>();
        ml.asyncReadEntry(position, new AsyncCallbacks.ReadEntryCallback() {
            @Override
            public void readEntryComplete(Entry entry, Object ctx) {
                entryFuture.complete(entry);
            }

            @Override
            public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                entryFuture.completeExceptionally(exception);
            }
        }, null);

        CompletableFuture<Integer> batchSizeFuture = entryFuture.thenApply(entry -> {
            PulsarApi.MessageMetadata metadata = parseMessageMetadata(entry.getDataBuffer());
            int batchSize = metadata.getNumMessagesInBatch();
            entry.release();
            return batchSize;
        });

        batchSizeFuture.whenComplete((batchSize, e) -> {
            if (e != null) {
                responseObserver.onNext(Commands.newError(
                        requestId, ServerError.MetadataError, "Failed to get batch size for entry " + e.getMessage()));
            } else {
                int largestBatchIndex = batchSize > 1 ? batchSize - 1 : -1;

                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}][{}] Get LastMessageId {} partitionIndex {}", remoteAddress,
                            topic.getName(), subscriptionName, position, partitionIndex);
                }

                MessageIdData messageId = MessageIdData.newBuilder()
                        .setLedgerId(position.getLedgerId())
                        .setEntryId(position.getEntryId())
                        .setPartition(partitionIndex)
                        .setBatchIndex(largestBatchIndex).build();

                responseObserver.onNext(Commands.newGetLastMessageIdResponse(requestId, messageId));
            }
        });
    }

    private SchemaData getSchema(Schema protocolSchema) {
        return SchemaData.builder()
                .data(protocolSchema.getSchemaData().toByteArray())
                .isDeleted(false)
                .timestamp(System.currentTimeMillis())
                //.user(Strings.nullToEmpty(originalPrincipal))
                .user("")
                .type(Commands.getSchemaType(protocolSchema.getType()))
                .props(protocolSchema.getPropertiesMap())
                .build();
    }

    private CompletableFuture<SchemaVersion> tryAddSchema(Topic topic, SchemaData schema, SocketAddress remoteAddress) {
        if (schema != null) {
            return topic.addSchema(schema);
        } else {
            return topic.hasSchema().thenCompose((hasSchema) -> {
                log.info("[{}] {} configured with schema {}",
                        remoteAddress, topic.getName(), hasSchema);
                CompletableFuture<SchemaVersion> result = new CompletableFuture<>();
                if (hasSchema && (configuration.isSchemaValidationEnforced() || topic.getSchemaValidationEnforced())) {
                    result.completeExceptionally(new IncompatibleSchemaException(
                            "Producers cannot connect or send message without a schema to topics with a schema"));
                } else {
                    result.complete(SchemaVersion.Empty);
                }
                return result;
            });
        }
    }

    private static void closeProduce(CompletableFuture<Producer> producerFuture, SocketAddress remoteAddress) {
        if (!producerFuture.isDone() && producerFuture
                .completeExceptionally(new IllegalStateException("Closed producer before creation was complete"))) {
            // We have received a request to close the producer before it was actually completed, we have marked the
            // producer future as failed and we can tell the client the close operation was successful.
            log.info("[{}] Closed producer before its creation was completed", remoteAddress);
            return;
        } else if (producerFuture.isCompletedExceptionally()) {
            log.info("[{}] Closed producer that already failed to be created", remoteAddress);
            return;
        }

        // Proceed with normal close, the producer
        Producer producer = producerFuture.getNow(null);
        log.info("[{}][{}] Closing producer on cnx {}", producer.getTopic(), producer.getProducerName(), remoteAddress);
        producer.close(true);
    }

    private static void closeConsume(CompletableFuture<Consumer> consumerFuture, SocketAddress remoteAddress,
            StreamObserver<ConsumeOutput> responseObserver) {

        if (!consumerFuture.isDone() && consumerFuture
                .completeExceptionally(new IllegalStateException("Closed consumer before creation was complete"))) {
            // We have received a request to close the consumer before it was actually completed, we have marked the
            // consumer future as failed and we can tell the client the close operation was successful. When the actual
            // create operation will complete, the new consumer will be discarded.
            log.info("[{}] Closed consumer before its creation was completed", remoteAddress);
            return;
        }

        if (consumerFuture.isCompletedExceptionally()) {
            log.info("[{}] Closed consumer that already failed to be created", remoteAddress);
            return;
        }

        // Proceed with normal consumer close
        Consumer consumer = consumerFuture.getNow(null);
        try {
            consumer.close();
            log.info("[{}] Closed consumer {}", remoteAddress, consumer);
            responseObserver.onCompleted();
        } catch (BrokerServiceException e) {
            log.warn("[{]] Error closing consumer {} : {}", remoteAddress, consumer, e);
            responseObserver.onError(newStatusException(Status.INTERNAL, e,
                    convertServerError(BrokerServiceException.getClientErrorCode(e))));
        }
    }

    private static class NoOpStreamObserver<T> implements StreamObserver<T> {

        public static <T> NoOpStreamObserver<T> create() {
            return new NoOpStreamObserver<>();
        }

        private NoOpStreamObserver() {
        }

        @Override
        public void onNext(T value) {
            // NoOp
        }

        @Override
        public void onError(Throwable t) {
            // NoOp
        }

        @Override
        public void onCompleted() {
            // NoOp
        }
    }

}
