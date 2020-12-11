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

import com.google.protobuf.ByteString;
import io.github.cbornet.pulsar.handlers.grpc.api.AuthData;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandAck;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandAck.AckType;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandAck.ValidationError;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandAckResponse;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandActiveConsumerChange;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandAddPartitionToTxn;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandAddPartitionToTxnResponse;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandAuthChallenge;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandConsumerStats;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandConsumerStatsResponse;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandEndTxn;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandEndTxnOnPartition;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandEndTxnOnPartitionResponse;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandEndTxnOnSubscription;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandEndTxnOnSubscriptionResponse;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandEndTxnResponse;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandError;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandFlow;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandGetLastMessageId;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandGetLastMessageIdResponse;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandGetOrCreateSchema;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandGetOrCreateSchemaResponse;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandGetSchema;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandGetSchemaResponse;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandGetTopicsOfNamespace;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandGetTopicsOfNamespaceResponse;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandLookupTopic;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandLookupTopicResponse;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandMessage;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandNewTxn;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandNewTxnResponse;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandPartitionedTopicMetadata;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandPartitionedTopicMetadataResponse;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandProducer;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandProducerSuccess;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandReachedEndOfTopic;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandRedeliverUnacknowledgedMessages;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandSeek;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandSend;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandSendError;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandSendReceipt;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandSubscribe;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandSubscribe.InitialPosition;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandSubscribe.SubType;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandSubscribeSuccess;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandSuccess;
import io.github.cbornet.pulsar.handlers.grpc.api.CommandUnsubscribe;
import io.github.cbornet.pulsar.handlers.grpc.api.CompressionType;
import io.github.cbornet.pulsar.handlers.grpc.api.ConsumeInput;
import io.github.cbornet.pulsar.handlers.grpc.api.ConsumeOutput;
import io.github.cbornet.pulsar.handlers.grpc.api.IntRange;
import io.github.cbornet.pulsar.handlers.grpc.api.KeySharedMeta;
import io.github.cbornet.pulsar.handlers.grpc.api.KeySharedMode;
import io.github.cbornet.pulsar.handlers.grpc.api.MessageIdData;
import io.github.cbornet.pulsar.handlers.grpc.api.MessageMetadata;
import io.github.cbornet.pulsar.handlers.grpc.api.Messages;
import io.github.cbornet.pulsar.handlers.grpc.api.MetadataAndPayload;
import io.github.cbornet.pulsar.handlers.grpc.api.PayloadType;
import io.github.cbornet.pulsar.handlers.grpc.api.PulsarGrpc;
import io.github.cbornet.pulsar.handlers.grpc.api.Schema;
import io.github.cbornet.pulsar.handlers.grpc.api.SendResult;
import io.github.cbornet.pulsar.handlers.grpc.api.ServerError;
import io.github.cbornet.pulsar.handlers.grpc.api.SingleMessage;
import io.github.cbornet.pulsar.handlers.grpc.api.SingleMessageMetadata;
import io.github.cbornet.pulsar.handlers.grpc.api.TxnAction;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.client.api.KeySharedPolicy;
import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.compression.CompressionCodec;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.common.policies.data.ConsumerStats;
import org.apache.pulsar.common.protocol.Commands.ChecksumType;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.common.util.SafeCollectionUtils;
import org.apache.pulsar.common.util.collections.BitSetRecyclable;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.github.cbornet.pulsar.handlers.grpc.Constants.CONSUMER_PARAMS_METADATA_KEY;
import static io.github.cbornet.pulsar.handlers.grpc.Constants.ERROR_CODE_METADATA_KEY;
import static io.github.cbornet.pulsar.handlers.grpc.Constants.PRODUCER_PARAMS_METADATA_KEY;
import static org.apache.pulsar.common.protocol.Commands.serializeMetadataAndPayload;

class Commands {

    public static final short MAGIC_CRC_32_C = 0x0e01;

    public static StatusRuntimeException newStatusException(Status status, String message, Throwable exception,
            ServerError code) {
        Metadata metadata = new Metadata();
        metadata.put(ERROR_CODE_METADATA_KEY, String.valueOf(code.getNumber()));
        return status.withDescription(message)
                .withCause(exception)
                .asRuntimeException(metadata);
    }

    public static StatusRuntimeException newStatusException(Status status, Throwable exception, ServerError code) {
        return newStatusException(status, exception.getMessage(), exception, code);
    }

    public static CommandAuthChallenge newAuthChallenge(String authMethod,
            org.apache.pulsar.common.api.AuthData brokerData, long stateId) {
        CommandAuthChallenge.Builder challengeBuilder = CommandAuthChallenge.newBuilder();
        challengeBuilder.setChallenge(AuthData.newBuilder()
                .setAuthData(ByteString.copyFrom(brokerData.getBytes()))
                .setAuthMethodName(authMethod)
                .setAuthStateId(stateId)
                .build());
        return challengeBuilder.build();
    }

    public static SendResult newProducerSuccess(String producerName, long lastSequenceId,
            SchemaVersion schemaVersion) {
        CommandProducerSuccess.Builder producerSuccessBuilder = CommandProducerSuccess.newBuilder();
        producerSuccessBuilder.setProducerName(producerName);
        producerSuccessBuilder.setLastSequenceId(lastSequenceId);
        producerSuccessBuilder.setSchemaVersion(ByteString.copyFrom(schemaVersion.bytes()));
        CommandProducerSuccess producerSuccess = producerSuccessBuilder.build();
        return SendResult.newBuilder().setProducerSuccess(producerSuccess).build();
    }

    public static CommandSend newSend(long sequenceId, int numMessages,
            PulsarApi.MessageMetadata messageMetadata, ByteBuf payload) {
        return newSend(sequenceId, numMessages,
                messageMetadata.hasTxnidLeastBits() ? messageMetadata.getTxnidLeastBits() : -1,
                messageMetadata.hasTxnidMostBits() ? messageMetadata.getTxnidMostBits() : -1,
                messageMetadata, payload);
    }

    public static CommandSend newSend(long lowestSequenceId, long highestSequenceId, int numMessages,
            PulsarApi.MessageMetadata messageMetadata, ByteBuf payload) {
        return newSend(lowestSequenceId, highestSequenceId, numMessages,
                messageMetadata.hasTxnidLeastBits() ? messageMetadata.getTxnidLeastBits() : -1,
                messageMetadata.hasTxnidMostBits() ? messageMetadata.getTxnidMostBits() : -1,
                messageMetadata, payload);
    }

    public static CommandSend newSend(long sequenceId, int numMessages,
            long txnIdLeastBits, long txnIdMostBits,
            PulsarApi.MessageMetadata messageData, ByteBuf payload) {
        CommandSend.Builder sendBuilder = CommandSend.newBuilder();
        sendBuilder.setSequenceId(sequenceId);
        if (numMessages > 1) {
            sendBuilder.setNumMessages(numMessages);
        }
        if (txnIdLeastBits >= 0) {
            sendBuilder.setTxnidLeastBits(txnIdLeastBits);
        }
        if (txnIdMostBits >= 0) {
            sendBuilder.setTxnidMostBits(txnIdMostBits);
        }
        if (messageData.hasTotalChunkMsgSize() && messageData.getTotalChunkMsgSize() > 1) {
            sendBuilder.setIsChunk(true);
        }
        ByteBuf headersAndPayloadByteBuf = serializeMetadataAndPayload(ChecksumType.Crc32c, messageData, payload);
        ByteString headersAndPayload = ByteString.copyFrom(headersAndPayloadByteBuf.nioBuffer());
        headersAndPayloadByteBuf.release();
        sendBuilder.setBinaryMetadataAndPayload(headersAndPayload);

        return sendBuilder.build();
    }

    public static CommandSend newSend(long lowestSequenceId, long highestSequenceId, int numMessages,
            long txnIdLeastBits, long txnIdMostBits,
            PulsarApi.MessageMetadata messageData, ByteBuf payload) {
        CommandSend.Builder sendBuilder = CommandSend.newBuilder();
        sendBuilder.setSequenceId(lowestSequenceId);
        sendBuilder.setHighestSequenceId(highestSequenceId);
        if (numMessages > 1) {
            sendBuilder.setNumMessages(numMessages);
        }
        if (messageData.hasTotalChunkMsgSize() && messageData.getTotalChunkMsgSize() > 1) {
            sendBuilder.setIsChunk(true);
        }
        ByteBuf headersAndPayloadByteBuf = serializeMetadataAndPayload(ChecksumType.Crc32c, messageData, payload);
        ByteString headersAndPayload = ByteString.copyFrom(headersAndPayloadByteBuf.nioBuffer());
        sendBuilder.setBinaryMetadataAndPayload(headersAndPayload);

        return sendBuilder.build();
    }

    public static SendResult newSendError(long sequenceId, ServerError error, String errorMsg) {
        CommandSendError.Builder sendErrorBuilder = CommandSendError.newBuilder();
        sendErrorBuilder.setSequenceId(sequenceId);
        sendErrorBuilder.setError(error);
        sendErrorBuilder.setMessage(errorMsg);
        CommandSendError sendError = sendErrorBuilder.build();
        return SendResult.newBuilder().setSendError(sendError).build();
    }

    public static SendResult newSendReceipt(long sequenceId, long highestId, long ledgerId, long entryId) {
        CommandSendReceipt.Builder sendReceiptBuilder = CommandSendReceipt.newBuilder();
        sendReceiptBuilder.setSequenceId(sequenceId);
        sendReceiptBuilder.setHighestSequenceId(highestId);
        MessageIdData.Builder messageIdBuilder = MessageIdData.newBuilder();
        messageIdBuilder.setLedgerId(ledgerId);
        messageIdBuilder.setEntryId(entryId);
        MessageIdData messageId = messageIdBuilder.build();
        sendReceiptBuilder.setMessageId(messageId);
        CommandSendReceipt sendReceipt = sendReceiptBuilder.build();
        return SendResult.newBuilder().setSendReceipt(sendReceipt).build();
    }

    public static CommandProducer newProducer(String topic, String producerName,
            boolean encrypted, Map<String, String> metadata, SchemaInfo schemaInfo,
            long epoch, boolean userProvidedProducerName) {
        CommandProducer.Builder producerBuilder = CommandProducer.newBuilder();
        producerBuilder.setTopic(topic);
        producerBuilder.setEpoch(epoch);
        if (producerName != null) {
            producerBuilder.setProducerName(producerName);
        }
        producerBuilder.setUserProvidedProducerName(userProvidedProducerName);
        producerBuilder.setEncrypted(encrypted);

        if (metadata != null) {
            producerBuilder.putAllMetadata(metadata);
        }

        if (null != schemaInfo) {
            producerBuilder.setSchema(getSchema(schemaInfo));
        }

        return producerBuilder.build();
    }

    public static CommandProducer newProducer(String topic, String producerName,
            Map<String, String> metadata) {
        return newProducer(topic, producerName, false, metadata);
    }

    public static CommandProducer newProducer(String topic, String producerName,
            boolean encrypted, Map<String, String> metadata) {
        return newProducer(topic, producerName, encrypted, metadata, null, 0, false);
    }

    private static Schema getSchema(SchemaInfo schemaInfo) {
        Schema.Builder builder = Schema.newBuilder()
                .setName(schemaInfo.getName())
                .setSchemaData(ByteString.copyFrom(schemaInfo.getSchema()))
                .setType(getSchemaType(schemaInfo.getType()))
                .putAllProperties(schemaInfo.getProperties());
        return builder.build();
    }

    public static Schema.Type getSchemaType(SchemaType type) {
        if (type.getValue() < 0) {
            return Schema.Type.None;
        } else {
            return Schema.Type.forNumber(type.getValue());
        }
    }

    public static SchemaType getSchemaType(Schema.Type type) {
        if (type.getNumber() < 0) {
            // this is unexpected
            return SchemaType.NONE;
        } else {
            return SchemaType.valueOf(type.getNumber());
        }
    }

    public static CommandGetSchema newGetSchema(String topic, SchemaVersion version) {
        CommandGetSchema.Builder schema = CommandGetSchema.newBuilder();
        schema.setTopic(topic);
        if (version != null) {
            schema.setSchemaVersion(ByteString.copyFrom(version.bytes()));
        }
        return schema.build();
    }

    public static CommandGetSchemaResponse newGetSchemaResponse(SchemaInfo schema, SchemaVersion version) {
        CommandGetSchemaResponse.Builder schemaResponse = CommandGetSchemaResponse.newBuilder()
                .setSchemaVersion(ByteString.copyFrom(version.bytes()))
                .setSchema(getSchema(schema));
        return schemaResponse.build();
    }

    public static CommandGetOrCreateSchema newGetOrCreateSchema(String topic, SchemaInfo schemaInfo) {
        return CommandGetOrCreateSchema.newBuilder()
                .setTopic(topic)
                .setSchema(getSchema(schemaInfo))
                .build();
    }

    public static CommandGetOrCreateSchemaResponse newGetOrCreateSchemaResponse(SchemaVersion schemaVersion) {
        CommandGetOrCreateSchemaResponse.Builder schemaResponse = CommandGetOrCreateSchemaResponse.newBuilder()
                .setSchemaVersion(ByteString.copyFrom(schemaVersion.bytes()));
        return schemaResponse.build();
    }

    public static CommandLookupTopic newLookup(String topic, boolean authoritative) {
        return newLookup(topic, null, authoritative);
    }

    public static CommandLookupTopic newLookup(String topic, String listenerName, boolean authoritative) {
        CommandLookupTopic.Builder lookupTopicBuilder = CommandLookupTopic.newBuilder();
        lookupTopicBuilder.setTopic(topic);
        lookupTopicBuilder.setAuthoritative(authoritative);
        if (StringUtils.isNotBlank(listenerName)) {
            lookupTopicBuilder.setAdvertisedListenerName(listenerName);
        }
        return lookupTopicBuilder.build();
    }

    public static CommandLookupTopicResponse newLookupResponse(String grpcServiceHost, Integer grpcServicePort,
            Integer grpcServicePortTls, boolean authoritative, CommandLookupTopicResponse.LookupType response,
            boolean proxyThroughServiceUrl) {
        CommandLookupTopicResponse.Builder commandLookupTopicResponseBuilder = CommandLookupTopicResponse.newBuilder();
        commandLookupTopicResponseBuilder.setGrpcServiceHost(grpcServiceHost);
        if (grpcServicePort != null) {
            commandLookupTopicResponseBuilder.setGrpcServicePort(grpcServicePort);
        }
        if (grpcServicePortTls != null) {
            commandLookupTopicResponseBuilder.setGrpcServicePortTls(grpcServicePortTls);
        }
        commandLookupTopicResponseBuilder.setResponse(response);
        commandLookupTopicResponseBuilder.setAuthoritative(authoritative);
        commandLookupTopicResponseBuilder.setProxyThroughServiceUrl(proxyThroughServiceUrl);

        return commandLookupTopicResponseBuilder.build();
    }

    public static CommandPartitionedTopicMetadata newPartitionMetadataRequest(String topic) {
        CommandPartitionedTopicMetadata.Builder partitionMetadataBuilder = CommandPartitionedTopicMetadata.newBuilder();
        partitionMetadataBuilder.setTopic(topic);
        return partitionMetadataBuilder.build();
    }

    public static CommandPartitionedTopicMetadataResponse newPartitionMetadataResponse(int partitions) {
        CommandPartitionedTopicMetadataResponse.Builder partitionMetadataResponseBuilder =
                CommandPartitionedTopicMetadataResponse.newBuilder();
        partitionMetadataResponseBuilder.setPartitions(partitions);
        return partitionMetadataResponseBuilder.build();
    }

    public static CommandSubscribe newSubscribe(String topic, String subscription,
            SubType subType, int priorityLevel, String consumerName, long resetStartMessageBackInSeconds) {
        return newSubscribe(topic, subscription, subType, priorityLevel, consumerName,
                true /* isDurable */, null /* startMessageId */, Collections.emptyMap(), false,
                false /* isReplicated */, InitialPosition.Earliest, resetStartMessageBackInSeconds, null,
                true /* createTopicIfDoesNotExist */, null);
    }

    public static CommandSubscribe newSubscribe(String topic, String subscription,
            SubType subType, int priorityLevel, String consumerName, long resetStartMessageBackInSeconds,
            PayloadType payloadType) {
        return newSubscribe(topic, subscription, subType, priorityLevel, consumerName,
                true /* isDurable */, null /* startMessageId */, Collections.emptyMap(), false,
                false /* isReplicated */, InitialPosition.Earliest, resetStartMessageBackInSeconds, null,
                true /* createTopicIfDoesNotExist */, payloadType);
    }

    public static CommandSubscribe newSubscribe(String topic, String subscription,
            SubType subType, int priorityLevel, String consumerName, boolean isDurable, MessageIdData startMessageId,
            Map<String, String> metadata, boolean readCompacted, boolean isReplicated,
            InitialPosition subscriptionInitialPosition, long startMessageRollbackDurationInSec, SchemaInfo schemaInfo,
            boolean createTopicIfDoesNotExist, PayloadType payloadType) {
        return newSubscribe(topic, subscription, subType, priorityLevel, consumerName,
                isDurable, startMessageId, metadata, readCompacted, isReplicated, subscriptionInitialPosition,
                startMessageRollbackDurationInSec, schemaInfo, createTopicIfDoesNotExist, null, payloadType);
    }

    public static CommandSubscribe newSubscribe(String topic, String subscription,
            SubType subType, int priorityLevel, String consumerName, boolean isDurable, MessageIdData startMessageId,
            Map<String, String> metadata, boolean readCompacted, boolean isReplicated,
            InitialPosition subscriptionInitialPosition, long startMessageRollbackDurationInSec,
            SchemaInfo schemaInfo, boolean createTopicIfDoesNotExist, KeySharedPolicy keySharedPolicy,
            PayloadType payloadType) {
        CommandSubscribe.Builder subscribeBuilder = CommandSubscribe.newBuilder();
        subscribeBuilder.setTopic(topic);
        subscribeBuilder.setSubscription(subscription);
        subscribeBuilder.setSubType(subType);
        subscribeBuilder.setConsumerName(consumerName);
        subscribeBuilder.setPriorityLevel(priorityLevel);
        subscribeBuilder.setDurable(isDurable);
        subscribeBuilder.setReadCompacted(readCompacted);
        subscribeBuilder.setInitialPosition(subscriptionInitialPosition);
        subscribeBuilder.setReplicateSubscriptionState(isReplicated);
        subscribeBuilder.setForceTopicCreation(createTopicIfDoesNotExist);

        if (keySharedPolicy != null) {
            KeySharedMeta.Builder keySharedMetaBuilder = KeySharedMeta.newBuilder();
            keySharedMetaBuilder.setAllowOutOfOrderDelivery(keySharedPolicy.isAllowOutOfOrderDelivery());
            keySharedMetaBuilder.setKeySharedMode(internalConvertKeySharedMode(keySharedPolicy.getKeySharedMode()));

            if (keySharedPolicy instanceof KeySharedPolicy.KeySharedPolicySticky) {
                List<Range> ranges = ((KeySharedPolicy.KeySharedPolicySticky) keySharedPolicy)
                        .getRanges();
                for (Range range : ranges) {
                    keySharedMetaBuilder.addHashRanges(IntRange.newBuilder()
                            .setStart(range.getStart())
                            .setEnd(range.getEnd()));
                }
            }

            subscribeBuilder.setKeySharedMeta(keySharedMetaBuilder.build());
        }

        if (startMessageId != null) {
            subscribeBuilder.setStartMessageId(startMessageId);
        }
        if (startMessageRollbackDurationInSec > 0) {
            subscribeBuilder.setStartMessageRollbackDurationSec(startMessageRollbackDurationInSec);
        }
        subscribeBuilder.putAllMetadata(metadata);

        Schema schema;
        if (schemaInfo != null) {
            schema = getSchema(schemaInfo);
            subscribeBuilder.setSchema(schema);
        }

        if (payloadType != null) {
            subscribeBuilder.setPreferedPayloadType(payloadType);
        }

        return subscribeBuilder.build();
    }

    private static KeySharedMode internalConvertKeySharedMode(org.apache.pulsar.client.api.KeySharedMode mode) {
        switch (mode) {
            case AUTO_SPLIT:
                return KeySharedMode.AUTO_SPLIT;
            case STICKY:
                return KeySharedMode.STICKY;
            default:
                throw new IllegalArgumentException("Unexpected key shared mode: " + mode);
        }
    }

    public static ConsumeOutput newSubscriptionSuccess() {
        return ConsumeOutput.newBuilder().setSubscribeSuccess(CommandSubscribeSuccess.newBuilder()).build();
    }

    public static ConsumeInput newUnsubscribe(long requestId) {
        CommandUnsubscribe.Builder unsubscribeBuilder = CommandUnsubscribe.newBuilder();
        unsubscribeBuilder.setRequestId(requestId);
        return ConsumeInput.newBuilder().setUnsubscribe(unsubscribeBuilder).build();
    }

    public static ConsumeOutput newSuccess(long requestId) {
        CommandSuccess.Builder successBuilder = CommandSuccess.newBuilder();
        successBuilder.setRequestId(requestId);
        return ConsumeOutput.newBuilder().setSuccess(successBuilder).build();
    }

    public static ConsumeOutput newError(long requestId, ServerError error, String message) {
        CommandError.Builder cmdErrorBuilder = CommandError.newBuilder();
        cmdErrorBuilder.setRequestId(requestId);
        cmdErrorBuilder.setError(error);
        cmdErrorBuilder.setMessage(message);
        return ConsumeOutput.newBuilder().setError(cmdErrorBuilder).build();
    }

    public static ConsumeInput newAck(MessageIdData messageIdData, CommandAck.AckType ackType) {
        return newAck(messageIdData.getLedgerId(), messageIdData.getEntryId(), null, ackType, null,
                Collections.emptyMap(), -1, -1, -1, -1);
    }

    public static ConsumeInput newAck(long ledgerId, long entryId, CommandAck.AckType ackType,
            CommandAck.ValidationError validationError, Map<String, Long> properties) {
        return newAck(ledgerId, entryId, null, ackType, validationError, properties, -1, -1, -1, -1);
    }

    public static ConsumeInput newAck(long ledgerId, long entryId, BitSetRecyclable ackSet, CommandAck.AckType ackType,
            CommandAck.ValidationError validationError, Map<String, Long> properties, long txnIdLeastBits,
            long txnIdMostBits, long requestId, int batchSize) {
        CommandAck.Builder ackBuilder = CommandAck.newBuilder();
        ackBuilder.setAckType(ackType);
        MessageIdData.Builder messageIdDataBuilder = MessageIdData.newBuilder();
        messageIdDataBuilder.setLedgerId(ledgerId);
        messageIdDataBuilder.setEntryId(entryId);
        if (ackSet != null) {
            messageIdDataBuilder.addAllAckSet(SafeCollectionUtils.longArrayToList(ackSet.toLongArray()));
        }
        if (batchSize >= 0) {
            messageIdDataBuilder.setBatchSize(batchSize);
        }
        MessageIdData messageIdData = messageIdDataBuilder.build();
        ackBuilder.addMessageId(messageIdData);
        if (validationError != null) {
            ackBuilder.setValidationError(validationError);
        }
        if (txnIdMostBits >= 0) {
            ackBuilder.setTxnidMostBits(txnIdMostBits);
        }
        if (txnIdLeastBits >= 0) {
            ackBuilder.setTxnidLeastBits(txnIdLeastBits);
        }
        if (requestId >= 0) {
            ackBuilder.setRequestId(requestId);
        }
        ackBuilder.putAllProperties(properties);
        return ConsumeInput.newBuilder().setAck(ackBuilder).build();
    }

    public static ConsumeOutput newAckResponse(long requestId, ServerError error, String errorMsg) {
        CommandAckResponse.Builder commandAckResponseBuilder = CommandAckResponse.newBuilder();
        commandAckResponseBuilder.setRequestId(requestId);

        if (error != null) {
            commandAckResponseBuilder.setError(error);
        }

        if (errorMsg != null) {
            commandAckResponseBuilder.setMessage(errorMsg);
        }

        return ConsumeOutput.newBuilder().setAckResponse(commandAckResponseBuilder).build();
    }

    public static ConsumeInput newFlow(int messagePermits) {
        CommandFlow.Builder flowBuilder = CommandFlow.newBuilder();
        flowBuilder.setMessagePermits(messagePermits);
        return ConsumeInput.newBuilder().setFlow(flowBuilder).build();
    }

    public static ConsumeOutput newActiveConsumerChange(boolean isActive) {
        CommandActiveConsumerChange.Builder changeBuilder = CommandActiveConsumerChange.newBuilder()
                .setIsActive(isActive);

        return ConsumeOutput.newBuilder().setActiveConsumerChange(changeBuilder).build();
    }

    public static ConsumeOutput newReachedEndOfTopic() {
        return ConsumeOutput.newBuilder().setReachedEndOfTopic(CommandReachedEndOfTopic.newBuilder()).build();
    }

    public static ConsumeOutput newMessage(MessageIdData.Builder messageIdBuilder, int redeliveryCount,
            ByteBuf metadataAndPayload, long[] ackSet, PayloadType preferedPayloadType) throws IOException {
        CommandMessage.Builder msgBuilder = CommandMessage.newBuilder();
        msgBuilder.setMessageId(messageIdBuilder);
        if (redeliveryCount > 0) {
            msgBuilder.setRedeliveryCount(redeliveryCount);
        }
        if (ackSet != null) {
            msgBuilder.addAllAckSet(SafeCollectionUtils.longArrayToList(ackSet));
        }
        if (preferedPayloadType == PayloadType.BINARY) {
            ByteString headersAndPayload = ByteString.copyFrom(metadataAndPayload.nioBuffer());
            msgBuilder.setBinaryMetadataAndPayload(headersAndPayload);
        } else {
            MessageMetadata metadata = parseMessageMetadata(metadataAndPayload);
            ByteBuf uncompressedPayload = metadataAndPayload;
            if (preferedPayloadType != PayloadType.METADATA_AND_PAYLOAD
                    && metadata.getCompression() != CompressionType.NONE
                    && metadata.getEncryptionKeysCount() == 0) {
                CompressionCodec compressor =
                        CompressionCodecProvider.getCompressionCodec(convertCompressionType(metadata.getCompression()));
                uncompressedPayload = compressor.decode(metadataAndPayload, metadata.getUncompressedSize());
            }
            if (preferedPayloadType == PayloadType.MESSAGES && metadata.getEncryptionKeysCount() == 0) {
                int batchSize = metadata.getNumMessagesInBatch();
                Messages.Builder messagesBuilder = Messages.newBuilder()
                        .setMetadata(metadata);
                for (int i = 0; i < batchSize; ++i) {
                    //if (log.isDebugEnabled()) {
                    //    log.debug("[{}] [{}] processing message num - {} in batch", subscription, consumerName, i);
                    //}
                    SingleMessageMetadata.Builder singleMessageMetadataBuilder = SingleMessageMetadata
                            .newBuilder();
                    ByteString singleMessagePayload;
                    if (metadata.hasNumMessagesInBatch()) {
                        singleMessagePayload = deSerializeSingleMessageInBatch(uncompressedPayload,
                                singleMessageMetadataBuilder, i, batchSize);
                    } else {
                        singleMessagePayload = ByteString.copyFrom(uncompressedPayload.nioBuffer());
                    }
                    if (!singleMessageMetadataBuilder.getCompactedOut()) {
                        SingleMessage.Builder singleMessageBuilder = SingleMessage.newBuilder()
                                .setMetadata(singleMessageMetadataBuilder)
                                .setPayload(singleMessagePayload);
                        messagesBuilder.addMessages(singleMessageBuilder);
                    }
                }
                msgBuilder.setMessages(messagesBuilder);
            } else {
                MetadataAndPayload.Builder metadataBuilder = MetadataAndPayload.newBuilder()
                        .setMetadata(metadata)
                        .setPayload(ByteString.copyFrom(uncompressedPayload.nioBuffer()));
                msgBuilder.setMetadataAndPayload(metadataBuilder);
            }
        }
        return ConsumeOutput.newBuilder().setMessage(msgBuilder).build();
    }

    public static boolean hasChecksum(ByteBuf buffer) {
        return buffer.getShort(buffer.readerIndex()) == MAGIC_CRC_32_C;
    }

    /**
     * Read the checksum and advance the reader index in the buffer.
     *
     * <p>Note: This method assume the checksum presence was already verified before.
     */
    public static int readChecksum(ByteBuf buffer) {
        buffer.skipBytes(2); //skip magic bytes
        return buffer.readInt();
    }

    public static void skipChecksumIfPresent(ByteBuf buffer) {
        if (hasChecksum(buffer)) {
            readChecksum(buffer);
        }
    }

    private static MessageMetadata parseMessageMetadata(ByteBuf buffer) {
        try {
            // initially reader-index may point to start_of_checksum : increment reader-index to start_of_metadata
            // to parse metadata
            skipChecksumIfPresent(buffer);
            int metadataSize = (int) buffer.readUnsignedInt();

            int writerIndex = buffer.writerIndex();
            buffer.writerIndex(buffer.readerIndex() + metadataSize);

            ByteBufInputStream stream = new ByteBufInputStream(buffer);
            MessageMetadata res = MessageMetadata.parseFrom(stream);

            buffer.writerIndex(writerIndex);
            stream.close();
            return res;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static ByteString deSerializeSingleMessageInBatch(ByteBuf uncompressedPayload,
            SingleMessageMetadata.Builder singleMessageMetadataBuilder, int index, int batchSize)
            throws IOException {
        int singleMetaSize = (int) uncompressedPayload.readUnsignedInt();
        int writerIndex = uncompressedPayload.writerIndex();
        int beginIndex = uncompressedPayload.readerIndex() + singleMetaSize;
        uncompressedPayload.writerIndex(beginIndex);

        ByteBufInputStream byteBufInputStream = new ByteBufInputStream(uncompressedPayload);
        singleMessageMetadataBuilder.mergeFrom(byteBufInputStream);
        byteBufInputStream.close();

        int singleMessagePayloadSize = singleMessageMetadataBuilder.getPayloadSize();

        int readerIndex = uncompressedPayload.readerIndex();
        //ByteBuf singleMessagePayload = uncompressedPayload.retainedSlice(readerIndex, singleMessagePayloadSize);
        ByteString singleMessagePayload =
                ByteString.copyFrom(uncompressedPayload.nioBuffer(readerIndex, singleMessagePayloadSize));
        uncompressedPayload.writerIndex(writerIndex);

        // reader now points to beginning of payload read; so move it past message payload just read
        if (index < batchSize) {
            uncompressedPayload.readerIndex(readerIndex + singleMessagePayloadSize);
        }

        return singleMessagePayload;
    }

    public static ConsumeInput newConsumerStats(long requestId) {
        CommandConsumerStats.Builder commandConsumerStatsBuilder = CommandConsumerStats.newBuilder();
        commandConsumerStatsBuilder.setRequestId(requestId);

        return ConsumeInput.newBuilder().setConsumerStats(commandConsumerStatsBuilder).build();
    }

    public static ConsumeOutput newConsumerStatsResponse(long requestId, ConsumerStats consumerStats,
            Subscription subscription) {
        CommandConsumerStatsResponse.Builder commandConsumerStatsResponseBuilder = CommandConsumerStatsResponse
                .newBuilder();
        commandConsumerStatsResponseBuilder.setRequestId(requestId);
        commandConsumerStatsResponseBuilder.setMsgRateOut(consumerStats.msgRateOut);
        commandConsumerStatsResponseBuilder.setMsgThroughputOut(consumerStats.msgThroughputOut);
        commandConsumerStatsResponseBuilder.setMsgRateRedeliver(consumerStats.msgRateRedeliver);
        commandConsumerStatsResponseBuilder.setConsumerName(consumerStats.consumerName);
        commandConsumerStatsResponseBuilder.setAvailablePermits(consumerStats.availablePermits);
        commandConsumerStatsResponseBuilder.setUnackedMessages(consumerStats.unackedMessages);
        commandConsumerStatsResponseBuilder.setBlockedConsumerOnUnackedMsgs(consumerStats.blockedConsumerOnUnackedMsgs);
        commandConsumerStatsResponseBuilder.setAddress(consumerStats.getAddress());
        commandConsumerStatsResponseBuilder.setConnectedSince(consumerStats.getConnectedSince());

        commandConsumerStatsResponseBuilder.setMsgBacklog(subscription.getNumberOfEntriesInBacklog(false));
        commandConsumerStatsResponseBuilder.setMsgRateExpired(subscription.getExpiredMessageRate());
        commandConsumerStatsResponseBuilder.setType(subscription.getTypeString());
        return ConsumeOutput.newBuilder().setConsumerStatsResponse(commandConsumerStatsResponseBuilder).build();
    }

    public static ConsumeInput newRedeliverUnacknowledgedMessages() {
        return ConsumeInput.newBuilder()
                .setRedeliverUnacknowledgedMessages(CommandRedeliverUnacknowledgedMessages.newBuilder())
                .build();
    }

    public static ConsumeInput newRedeliverUnacknowledgedMessages(List<MessageIdData> messageIds) {
        CommandRedeliverUnacknowledgedMessages.Builder redeliverBuilder = CommandRedeliverUnacknowledgedMessages
                .newBuilder();
        redeliverBuilder.addAllMessageIds(messageIds);
        return ConsumeInput.newBuilder().setRedeliverUnacknowledgedMessages(redeliverBuilder).build();
    }

    public static ConsumeInput newGetLastMessageId(long requestId) {
        CommandGetLastMessageId.Builder cmdBuilder = CommandGetLastMessageId.newBuilder();
        cmdBuilder.setRequestId(requestId);

        return ConsumeInput.newBuilder().setGetLastMessageId(cmdBuilder).build();
    }

    public static ConsumeOutput newGetLastMessageIdResponse(long requestId, MessageIdData messageIdData) {
        CommandGetLastMessageIdResponse.Builder response =
                CommandGetLastMessageIdResponse.newBuilder()
                        .setLastMessageId(messageIdData)
                        .setRequestId(requestId);

        return ConsumeOutput.newBuilder().setGetLastMessageIdResponse(response).build();
    }

    public static ConsumeInput newSeek(long requestId, long ledgerId, long entryId, long[] ackSet) {
        CommandSeek.Builder seekBuilder = CommandSeek.newBuilder();
        seekBuilder.setRequestId(requestId);

        MessageIdData.Builder messageIdBuilder = MessageIdData.newBuilder();
        messageIdBuilder.setLedgerId(ledgerId);
        messageIdBuilder.setEntryId(entryId);
        messageIdBuilder.addAllAckSet(SafeCollectionUtils.longArrayToList(ackSet));
        MessageIdData messageId = messageIdBuilder.build();
        seekBuilder.setMessageId(messageId);

        return ConsumeInput.newBuilder().setSeek(seekBuilder).build();
    }

    public static ConsumeInput newSeek(long requestId, long timestamp) {
        CommandSeek.Builder seekBuilder = CommandSeek.newBuilder();
        seekBuilder.setRequestId(requestId);
        seekBuilder.setMessagePublishTime(timestamp);

        return ConsumeInput.newBuilder().setSeek(seekBuilder).build();
    }

    public static CommandGetTopicsOfNamespace newGetTopicsOfNamespaceRequest(String namespace,
            CommandGetTopicsOfNamespace.Mode mode) {
        CommandGetTopicsOfNamespace.Builder topicsBuilder = CommandGetTopicsOfNamespace.newBuilder();
        topicsBuilder.setNamespace(namespace).setMode(mode);

        return topicsBuilder.build();
    }

    public static CommandGetTopicsOfNamespaceResponse newGetTopicsOfNamespaceResponse(List<String> topics) {
        CommandGetTopicsOfNamespaceResponse.Builder topicsResponseBuilder =
                CommandGetTopicsOfNamespaceResponse.newBuilder();

        topicsResponseBuilder.addAllTopics(topics);

        return topicsResponseBuilder.build();
    }

    public static CommandNewTxn newTxn(long tcId) {
        return CommandNewTxn.newBuilder()
                .setTcId(tcId)
                .build();
    }

    public static CommandNewTxnResponse newTxnResponse(long leastSigBits, long mostSigBits) {
        return CommandNewTxnResponse.newBuilder()
                .setTxnidLeastBits(leastSigBits)
                .setTxnidMostBits(mostSigBits)
                .build();
    }

    public static CommandAddPartitionToTxn newAddPartitionToTxn(long txnIdLeastBits, long txnIdMostBits,
            Iterable<String> partitions) {
        return CommandAddPartitionToTxn.newBuilder()
                .setTxnidLeastBits(txnIdLeastBits)
                .setTxnidMostBits(txnIdMostBits)
                .addAllPartitions(partitions)
                .build();
    }

    public static CommandAddPartitionToTxnResponse newAddPartitionToTxnResponse(long txnIdLeastBits,
            long txnIdMostBits) {
        return CommandAddPartitionToTxnResponse.newBuilder()
                .setTxnidLeastBits(txnIdLeastBits)
                .setTxnidMostBits(txnIdMostBits)
                .build();
    }

    public static CommandEndTxn newEndTxn(long txnIdLeastBits, long txnIdMostBits, TxnAction txnAction) {
        return CommandEndTxn.newBuilder()
                .setTxnidLeastBits(txnIdLeastBits)
                .setTxnidMostBits(txnIdMostBits)
                .setTxnAction(txnAction)
                .build();
    }

    public static CommandEndTxnResponse newEndTxnResponse(long txnIdLeastBits, long txnIdMostBits) {
        return CommandEndTxnResponse.newBuilder()
                .setTxnidLeastBits(txnIdLeastBits)
                .setTxnidMostBits(txnIdMostBits).build();
    }

    public static CommandEndTxnOnPartition newEndTxnOnPartition(long txnIdLeastBits, long txnIdMostBits, String topic,
            TxnAction txnAction, List<MessageIdData> messageIdDataList) {
        return CommandEndTxnOnPartition.newBuilder()
                .setTxnidLeastBits(txnIdLeastBits)
                .setTxnidMostBits(txnIdMostBits)
                .setTopic(topic)
                .setTxnAction(txnAction)
                .addAllMessageId(messageIdDataList)
                .build();
    }

    public static CommandEndTxnOnPartitionResponse newEndTxnOnPartitionResponse(long txnIdLeastBits,
            long txnIdMostBits) {
        return CommandEndTxnOnPartitionResponse.newBuilder()
                .setTxnidLeastBits(txnIdLeastBits)
                .setTxnidMostBits(txnIdMostBits)
                .build();
    }

    public static CommandEndTxnOnSubscription newEndTxnOnSubscription(long txnIdLeastBits, long txnIdMostBits,
            String topic,
            String subscription, TxnAction txnAction) {
        io.github.cbornet.pulsar.handlers.grpc.api.Subscription sub =
                io.github.cbornet.pulsar.handlers.grpc.api.Subscription.newBuilder()
                        .setTopic(topic)
                        .setSubscription(subscription)
                        .build();
        return newEndTxnOnSubscription(txnIdLeastBits, txnIdMostBits, sub, txnAction);
    }

    public static CommandEndTxnOnSubscription newEndTxnOnSubscription(long txnIdLeastBits, long txnIdMostBits,
            io.github.cbornet.pulsar.handlers.grpc.api.Subscription subscription, TxnAction txnAction) {
        return CommandEndTxnOnSubscription.newBuilder()
                .setTxnidLeastBits(txnIdLeastBits)
                .setTxnidMostBits(txnIdMostBits)
                .setSubscription(subscription)
                .setTxnAction(txnAction)
                .build();
    }

    public static CommandEndTxnOnSubscriptionResponse newEndTxnOnSubscriptionResponse(long txnIdLeastBits,
            long txnIdMostBits) {
        return CommandEndTxnOnSubscriptionResponse.newBuilder()
                .setTxnidLeastBits(txnIdLeastBits)
                .setTxnidMostBits(txnIdMostBits).build();
    }

    public static PulsarGrpc.PulsarStub attachProducerParams(PulsarGrpc.PulsarStub stub,
            CommandProducer producerParams) {
        Metadata headers = new Metadata();
        headers.put(PRODUCER_PARAMS_METADATA_KEY, producerParams.toByteArray());
        return MetadataUtils.attachHeaders(stub, headers);
    }

    public static PulsarGrpc.PulsarStub attachConsumerParams(PulsarGrpc.PulsarStub stub,
            CommandSubscribe consumerParams) {
        Metadata headers = new Metadata();
        headers.put(CONSUMER_PARAMS_METADATA_KEY, consumerParams.toByteArray());
        return MetadataUtils.attachHeaders(stub, headers);
    }

    public static ServerError convertServerError(PulsarApi.ServerError serverError) {
        if (serverError == null) {
            return null;
        }
        switch (serverError) {
            case MetadataError:
                return ServerError.MetadataError;
            case PersistenceError:
                return ServerError.PersistenceError;
            case AuthenticationError:
                return ServerError.AuthenticationError;
            case AuthorizationError:
                return ServerError.AuthorizationError;
            case ConsumerBusy:
                return ServerError.ConsumerBusy;
            case ServiceNotReady:
                return ServerError.ServiceNotReady;
            case ProducerBlockedQuotaExceededError:
                return ServerError.ProducerBlockedQuotaExceededError;
            case ProducerBlockedQuotaExceededException:
                return ServerError.ProducerBlockedQuotaExceededException;
            case ChecksumError:
                return ServerError.ChecksumError;
            case UnsupportedVersionError:
                return ServerError.UnsupportedVersionError;
            case TopicNotFound:
                return ServerError.TopicNotFound;
            case SubscriptionNotFound:
                return ServerError.SubscriptionNotFound;
            case ConsumerNotFound:
                return ServerError.ConsumerNotFound;
            case TooManyRequests:
                return ServerError.TooManyRequests;
            case TopicTerminatedError:
                return ServerError.TopicTerminatedError;
            case ProducerBusy:
                return ServerError.ProducerBusy;
            case InvalidTopicName:
                return ServerError.InvalidTopicName;
            case IncompatibleSchema:
                return ServerError.IncompatibleSchema;
            case ConsumerAssignError:
                return ServerError.ConsumerAssignError;
            case TransactionCoordinatorNotFound:
                return ServerError.TransactionCoordinatorNotFound;
            case InvalidTxnStatus:
                return ServerError.InvalidTxnStatus;
            case NotAllowedError:
                return ServerError.NotAllowedError;
            case TransactionConflict:
                return ServerError.TransactionConflict;
            case UnknownError:
            default:
                return ServerError.UnknownError;
        }
    }

    public static PulsarApi.CommandSubscribe.SubType convertSubscribeSubType(SubType subType) {
        if (subType == null) {
            return null;
        }
        switch (subType) {
            case Shared:
                return PulsarApi.CommandSubscribe.SubType.Shared;
            case Failover:
                return PulsarApi.CommandSubscribe.SubType.Failover;
            case Exclusive:
                return PulsarApi.CommandSubscribe.SubType.Exclusive;
            case Key_Shared:
                return PulsarApi.CommandSubscribe.SubType.Key_Shared;
            default:
                throw new IllegalStateException("Unexpected subscribe subtype: " + subType);
        }
    }

    public static PulsarApi.CommandSubscribe.InitialPosition convertSubscribeInitialPosition(
            InitialPosition initialPosition) {
        if (initialPosition == null) {
            return null;
        }
        switch (initialPosition) {
            case Latest:
                return PulsarApi.CommandSubscribe.InitialPosition.Latest;
            case Earliest:
                return PulsarApi.CommandSubscribe.InitialPosition.Earliest;
            default:
                throw new IllegalStateException("Unexpected subscribe initial position : " + initialPosition);
        }
    }

    public static PulsarApi.KeySharedMode convertKeySharedMode(KeySharedMode mode) {
        if (mode == null) {
            return null;
        }
        switch (mode) {
            case STICKY:
                return PulsarApi.KeySharedMode.STICKY;
            case AUTO_SPLIT:
                return PulsarApi.KeySharedMode.AUTO_SPLIT;
            default:
                throw new IllegalStateException("Unexpected key shared mode: " + mode);
        }
    }

    public static PulsarApi.KeySharedMeta convertKeySharedMeta(KeySharedMeta meta) {
        if (meta == null) {
            return null;
        }
        PulsarApi.KeySharedMeta.Builder builder = PulsarApi.KeySharedMeta.newBuilder()
                .setKeySharedMode(convertKeySharedMode(meta.getKeySharedMode()));
        for (IntRange intRange : meta.getHashRangesList()) {
            PulsarApi.IntRange.Builder hashRangeBuilder = PulsarApi.IntRange.newBuilder()
                    .setStart(intRange.getStart())
                    .setEnd(intRange.getEnd());
            builder.addHashRanges(hashRangeBuilder);
            hashRangeBuilder.recycle();
        }
        PulsarApi.KeySharedMeta keySharedMeta = builder.build();
        builder.recycle();
        return keySharedMeta;
    }

    public static PulsarApi.CommandAck.AckType convertAckType(AckType type) {
        if (type == null) {
            return null;
        }
        switch (type) {
            case Individual:
                return PulsarApi.CommandAck.AckType.Individual;
            case Cumulative:
                return PulsarApi.CommandAck.AckType.Cumulative;
            default:
                throw new IllegalStateException("Unexpected ack type: " + type);
        }
    }

    public static PulsarApi.CommandAck.ValidationError convertValidationError(ValidationError error) {
        if (error == null) {
            return null;
        }
        switch (error) {
            case DecryptionError:
                return PulsarApi.CommandAck.ValidationError.DecryptionError;
            case ChecksumMismatch:
                return PulsarApi.CommandAck.ValidationError.ChecksumMismatch;
            case DecompressionError:
                return PulsarApi.CommandAck.ValidationError.DecompressionError;
            case BatchDeSerializeError:
                return PulsarApi.CommandAck.ValidationError.BatchDeSerializeError;
            case UncompressedSizeCorruption:
                return PulsarApi.CommandAck.ValidationError.UncompressedSizeCorruption;
            default:
                throw new IllegalStateException("Unexpected ack validation error: " + error);
        }
    }

    public static PulsarApi.MessageIdData convertMessageIdData(MessageIdData messageIdData) {
        if (messageIdData == null) {
            return null;
        }
        PulsarApi.MessageIdData.Builder builder = PulsarApi.MessageIdData.newBuilder()
                .setEntryId(messageIdData.getEntryId())
                .setLedgerId(messageIdData.getLedgerId());
        if (messageIdData.hasPartition()) {
            builder.setPartition(messageIdData.getPartition());
        }
        if (messageIdData.hasBatchIndex()) {
            builder.setBatchIndex(messageIdData.getBatchIndex());
        }
        builder.addAllAckSet(messageIdData.getAckSetList());
        if (messageIdData.hasBatchSize()) {
            builder.setBatchSize(messageIdData.getBatchSize());
        }
        PulsarApi.MessageIdData result = builder.build();
        builder.recycle();
        return result;
    }

    public static PulsarApi.CommandAck convertCommandAck(CommandAck ack) {
        if (ack == null) {
            return null;
        }
        PulsarApi.CommandAck.Builder builder = PulsarApi.CommandAck.newBuilder()
                .setAckType(convertAckType(ack.getAckType()))
                .setConsumerId(0L);
        if (ack.hasValidationError()) {
            builder.setValidationError(convertValidationError(ack.getValidationError()));
        }
        if (ack.hasTxnidLeastBits()) {
            builder.setTxnidLeastBits(ack.getTxnidLeastBits());
        }
        if (ack.hasTxnidMostBits()) {
            builder.setTxnidMostBits(ack.getTxnidMostBits());
        }
        if (ack.hasRequestId()) {
            builder.setRequestId(ack.getRequestId());
        }
        ack.getPropertiesMap().forEach((k, v) -> {
            PulsarApi.KeyLongValue.Builder keyLongValue = PulsarApi.KeyLongValue.newBuilder()
                    .setKey(k)
                    .setValue(v);
            builder.addProperties(keyLongValue);
            keyLongValue.recycle();
        });
        for (MessageIdData messageIdData : ack.getMessageIdList()) {
            PulsarApi.MessageIdData idData = convertMessageIdData(messageIdData);
            builder.addMessageId(idData);
        }
        PulsarApi.CommandAck result = builder.build();
        builder.recycle();
        return result;
    }

    public static PulsarApi.CommandGetTopicsOfNamespace.Mode convertGetTopicsOfNamespaceMode(
            CommandGetTopicsOfNamespace.Mode mode) {
        if (mode == null) {
            return null;
        }
        switch (mode) {
            case NON_PERSISTENT:
                return PulsarApi.CommandGetTopicsOfNamespace.Mode.NON_PERSISTENT;
            case PERSISTENT:
                return PulsarApi.CommandGetTopicsOfNamespace.Mode.PERSISTENT;
            case ALL:
                return PulsarApi.CommandGetTopicsOfNamespace.Mode.ALL;
            default:
                throw new IllegalStateException("Unexpected GetTopicsOfNamespace mode: " + mode);
        }
    }

    public static PulsarApi.CompressionType convertCompressionType(CompressionType type) {
        if (type == null) {
            return null;
        }
        switch (type) {
            case NONE:
                return PulsarApi.CompressionType.NONE;
            case LZ4:
                return PulsarApi.CompressionType.LZ4;
            case ZLIB:
                return PulsarApi.CompressionType.ZLIB;
            case ZSTD:
                return PulsarApi.CompressionType.ZSTD;
            case SNAPPY:
                return PulsarApi.CompressionType.SNAPPY;
            default:
                throw new IllegalStateException("Unexpected compression type: " + type);
        }
    }

    public static CompressionType convertCompressionType(PulsarApi.CompressionType type) {
        if (type == null) {
            return null;
        }
        switch (type) {
            case NONE:
                return CompressionType.NONE;
            case LZ4:
                return CompressionType.LZ4;
            case ZLIB:
                return CompressionType.ZLIB;
            case ZSTD:
                return CompressionType.ZSTD;
            case SNAPPY:
                return CompressionType.SNAPPY;
            default:
                throw new IllegalStateException("Unexpected compression type: " + type);
        }
    }

    public static PulsarApi.SingleMessageMetadata.Builder convertSingleMessageMetadata(
            SingleMessageMetadata messageMetadata) {
        PulsarApi.SingleMessageMetadata.Builder builder = PulsarApi.SingleMessageMetadata.newBuilder();

        if (messageMetadata.hasPartitionKey()) {
            builder.setPartitionKey(messageMetadata.getPartitionKey());
        }
        if (messageMetadata.hasCompactedOut()) {
            builder.setCompactedOut(messageMetadata.getCompactedOut());
        }
        if (messageMetadata.hasEventTime()) {
            builder.setEventTime(messageMetadata.getEventTime());
        }
        if (messageMetadata.hasPartitionKeyB64Encoded()) {
            builder.setPartitionKeyB64Encoded(messageMetadata.getPartitionKeyB64Encoded());
        }
        if (messageMetadata.hasOrderingKey()) {
            builder.setOrderingKey(org.apache.pulsar.shaded.com.google.protobuf.v241.ByteString
                    .copyFrom(messageMetadata.getOrderingKey().asReadOnlyByteBuffer()));
        }
        if (messageMetadata.hasSequenceId()) {
            builder.setSequenceId(messageMetadata.getSequenceId());
        }
        if (messageMetadata.hasNullValue()) {
            builder.setNullValue(messageMetadata.getNullValue());
        }
        if (messageMetadata.hasNullPartitionKey()) {
            builder.setNullPartitionKey(messageMetadata.getNullPartitionKey());
        }
        messageMetadata.getPropertiesMap().forEach(
                (k, v) -> builder.addProperties(PulsarApi.KeyValue.newBuilder().setKey(k).setValue(v))
        );
        return builder;
    }
}
