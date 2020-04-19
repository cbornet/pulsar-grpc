/**
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

import com.google.common.base.Strings;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.netty.channel.EventLoopGroup;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.service.*;
import org.apache.pulsar.broker.service.schema.SchemaRegistryService;
import org.apache.pulsar.broker.service.schema.exceptions.IncompatibleSchemaException;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.naming.Metadata;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.protocol.schema.SchemaInfoUtil;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.protocols.grpc.api.CommandSubscribe.SubType;
import org.apache.pulsar.protocols.grpc.api.CommandSubscribe.InitialPosition;
import org.apache.pulsar.protocols.grpc.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;

import static org.apache.pulsar.protocols.grpc.Commands.newStatusException;
import static org.apache.pulsar.protocols.grpc.Constants.*;
import static org.apache.pulsar.protocols.grpc.ServerErrors.*;
import static org.apache.pulsar.protocols.grpc.TopicLookup.lookupTopicAsync;

public class PulsarGrpcService extends PulsarGrpc.PulsarImplBase {

    private static final Logger log = LoggerFactory.getLogger(PulsarGrpcService.class);

    private final BrokerService service;
    private final SchemaRegistryService schemaService;
    private final EventLoopGroup eventLoopGroup;
    private final boolean schemaValidationEnforced;
    private String originalPrincipal = null;

    public PulsarGrpcService(BrokerService service, ServiceConfiguration configuration, EventLoopGroup eventLoopGroup) {
        this.service = service;
        this.schemaService = service.pulsar().getSchemaRegistryService();
        this.eventLoopGroup = eventLoopGroup;
        this.schemaValidationEnforced = configuration.isSchemaValidationEnforced();
    }

    @Override
    public void lookupTopic(CommandLookupTopic lookup, StreamObserver<CommandLookupTopicResponse> responseObserver) {
        final SocketAddress remoteAddress = REMOTE_ADDRESS_CTX_KEY.get();
        final String authRole = AUTH_ROLE_CTX_KEY.get();
        final AuthenticationDataSource authenticationData = AUTH_DATA_CTX_KEY.get();
        final boolean authoritative = lookup.getAuthoritative();

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
            lookupTopicAsync(service.pulsar(), topicName, authoritative, authRole, authenticationData)
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
            log.debug("Received CommandGetSchema call from {}", remoteAddress);
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
    public StreamObserver<CommandSend> produce(StreamObserver<SendResult> responseObserver) {
        final CommandProducer cmdProducer = PRODUCER_PARAMS_CTX_KEY.get();
        if (cmdProducer == null) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Missing CommandProducer header").asException());
            return NoOpStreamObserver.create();
        }

        final String authRole = AUTH_ROLE_CTX_KEY.get();
        final AuthenticationDataSource authenticationData = AUTH_DATA_CTX_KEY.get();
        final SocketAddress remoteAddress = REMOTE_ADDRESS_CTX_KEY.get();

        final String topic = cmdProducer.getTopic();
        // Use producer name provided by client if present
        final String producerName = cmdProducer.hasProducerName() ? cmdProducer.getProducerName()
                : service.generateUniqueProducerName();
        final long epoch = cmdProducer.getEpoch();
        final boolean userProvidedProducerName = cmdProducer.getUserProvidedProducerName();
        final boolean isEncrypted = cmdProducer.getEncrypted();
        final Map<String, String> metadata = cmdProducer.getMetadataMap();
        final SchemaData schema = cmdProducer.hasSchema() ? getSchema(cmdProducer.getSchema()) : null;

        GrpcCnx cnx = new GrpcCnx(service, remoteAddress, authRole, authenticationData,
                responseObserver, eventLoopGroup.next());

        TopicName topicName;
        try {
            topicName = TopicName.get(topic);
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Failed to parse topic name '{}'", remoteAddress, topic, e);
            }
            responseObserver.onError(newStatusException(Status.INVALID_ARGUMENT,
                    "Invalid topic name: " + e.getMessage(), e, ServerError.InvalidTopicName));
            return NoOpStreamObserver.create();
        }

        // TODO: add authorization
        CompletableFuture<Producer> producerFuture = new CompletableFuture<>();
        service.getOrCreateTopic(topicName.toString()).thenAccept((Topic topik) -> {
            // Before creating producer, check if backlog quota exceeded on topic
            if (topik.isBacklogQuotaExceeded(producerName)) {
                IllegalStateException illegalStateException = new IllegalStateException(
                        "Cannot create producer on topic with backlog quota exceeded");
                BacklogQuota.RetentionPolicy retentionPolicy = topik.getBacklogQuota().getPolicy();
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
            if (topik.isEncryptionRequired() && !isEncrypted) {
                String msg = String.format("Encryption is required in %s", topicName);
                log.warn("[{}] {}", remoteAddress, msg);
                responseObserver.onError(newStatusException(Status.INVALID_ARGUMENT, msg, null,
                        ServerError.MetadataError));
                return;
            }

            CompletableFuture<SchemaVersion> schemaVersionFuture = tryAddSchema(topik, schema, remoteAddress);

            schemaVersionFuture.exceptionally(exception -> {
                responseObserver.onError(newStatusException(Status.FAILED_PRECONDITION, exception,
                        convertServerError(BrokerServiceException.getClientErrorCode(exception))));
                return null;
            });

            schemaVersionFuture.thenAccept(schemaVersion -> {
                Producer producer = new Producer(topik, cnx, 0L, producerName, authRole,
                        isEncrypted, metadata, schemaVersion, epoch, userProvidedProducerName);

                try {
                    // TODO : check that removeProducer is called even with early client disconnect
                    topik.addProducer(producer);
                    log.info("[{}] Created new producer: {}", remoteAddress, producer);
                    producerFuture.complete(producer);
                    responseObserver.onNext(Commands.newProducerSuccess(producerName,
                            producer.getLastSequenceId(), producer.getSchemaVersion()));
                } catch (BrokerServiceException ise) {
                    log.error("[{}] Failed to add producer to topic {}: {}", remoteAddress, topicName,
                            ise.getMessage());
                    responseObserver.onError(newStatusException(Status.FAILED_PRECONDITION, ise,
                            convertServerError(BrokerServiceException.getClientErrorCode(ise))));
                    producerFuture.completeExceptionally(ise);
                }
            });
        }).exceptionally(exception -> {
            Throwable cause = exception.getCause();
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
            }

            @Override
            public void onCompleted() {
                closeProduce(producerFuture, remoteAddress);
            }
        };
    }

    @Override
    public StreamObserver<ConsumeInput> consume(StreamObserver<ConsumeOutput> responseObserver) {
        final CommandSubscribe subscribe = CONSUMER_PARAMS_CTX_KEY.get();
        final String authRole = AUTH_ROLE_CTX_KEY.get();
        final AuthenticationDataSource authenticationData = AUTH_DATA_CTX_KEY.get();
        final SocketAddress remoteAddress = REMOTE_ADDRESS_CTX_KEY.get();

        if (subscribe == null) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Missing CommandSubscribe header").asException());
            return NoOpStreamObserver.create();
        }

        final long requestId = subscribe.getRequestId();
        final long consumerId = subscribe.getConsumerId();

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

        GrpcConsumerCnx cnx = new GrpcConsumerCnx(service, remoteAddress, authRole, authenticationData, responseObserver);

        CompletableFuture<Boolean> authorizationFuture;
        if (service.isAuthorizationEnabled()) {
            authorizationFuture = service.getAuthorizationService().canConsumeAsync(topicName, authRole, authenticationData,
                    subscriptionName);
        } else {
            authorizationFuture = CompletableFuture.completedFuture(true);
        }

        authorizationFuture.thenApply(isAuthorized -> {
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
                CompletableFuture<Consumer> consumerFuture = new CompletableFuture<>();


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
                                        .thenCompose(v -> topic.subscribe(cnx, subscriptionName, consumerId,
                                                convertSubscribeSubType(subType), priorityLevel,
                                                consumerName, isDurable, startMessageId, metadata, readCompacted,
                                                convertSubscribeInitialPosition(initialPosition),
                                                startMessageRollbackDurationSec, isReplicated,
                                                convertKeySharedMeta(keySharedMeta)));
                            } else {
                                return topic.subscribe(cnx, subscriptionName, consumerId,
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

                            // If client timed out, the future would have been completed by subsequent close.
                            // Send error
                            // back to client, only if not completed already.
                            if (consumerFuture.completeExceptionally(exception)) {
                                responseObserver.onError(newStatusException(Status.FAILED_PRECONDITION, exception.getCause(),
                                        convertServerError(BrokerServiceException.getClientErrorCode(exception))));
                            }

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

            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onCompleted() {

            }
        };
    }

    private SchemaData getSchema(Schema protocolSchema) {
        return SchemaData.builder()
                .data(protocolSchema.getSchemaData().toByteArray())
                .isDeleted(false)
                .timestamp(System.currentTimeMillis())
                .user(Strings.nullToEmpty(originalPrincipal))
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
                if (hasSchema && (schemaValidationEnforced || topic.getSchemaValidationEnforced())) {
                    result.completeExceptionally(new IncompatibleSchemaException(
                            "Producers cannot connect or send message without a schema to topics with a schema"));
                } else {
                    result.complete(SchemaVersion.Empty);
                }
                return result;
            });
        }
    }

    private void closeProduce(CompletableFuture<Producer> producerFuture, SocketAddress remoteAddress) {
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

    private static class NoOpStreamObserver<T> implements StreamObserver<T> {

        public static <T> NoOpStreamObserver<T> create() {
            return new NoOpStreamObserver<T>();
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
