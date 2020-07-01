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

import com.google.protobuf.ByteString;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
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
import org.apache.pulsar.common.util.protobuf.ByteBufCodedInputStream;
import org.apache.pulsar.protocols.grpc.api.AuthData;
import org.apache.pulsar.protocols.grpc.api.CommandAck;
import org.apache.pulsar.protocols.grpc.api.CommandAck.AckType;
import org.apache.pulsar.protocols.grpc.api.CommandAck.ValidationError;
import org.apache.pulsar.protocols.grpc.api.CommandActiveConsumerChange;
import org.apache.pulsar.protocols.grpc.api.CommandAddPartitionToTxn;
import org.apache.pulsar.protocols.grpc.api.CommandAddPartitionToTxnResponse;
import org.apache.pulsar.protocols.grpc.api.CommandAuthChallenge;
import org.apache.pulsar.protocols.grpc.api.CommandConsumerStats;
import org.apache.pulsar.protocols.grpc.api.CommandConsumerStatsResponse;
import org.apache.pulsar.protocols.grpc.api.CommandEndTxn;
import org.apache.pulsar.protocols.grpc.api.CommandEndTxnResponse;
import org.apache.pulsar.protocols.grpc.api.CommandError;
import org.apache.pulsar.protocols.grpc.api.CommandFlow;
import org.apache.pulsar.protocols.grpc.api.CommandGetLastMessageId;
import org.apache.pulsar.protocols.grpc.api.CommandGetLastMessageIdResponse;
import org.apache.pulsar.protocols.grpc.api.CommandGetOrCreateSchema;
import org.apache.pulsar.protocols.grpc.api.CommandGetOrCreateSchemaResponse;
import org.apache.pulsar.protocols.grpc.api.CommandGetSchema;
import org.apache.pulsar.protocols.grpc.api.CommandGetSchemaResponse;
import org.apache.pulsar.protocols.grpc.api.CommandGetTopicsOfNamespace;
import org.apache.pulsar.protocols.grpc.api.CommandGetTopicsOfNamespaceResponse;
import org.apache.pulsar.protocols.grpc.api.CommandLookupTopic;
import org.apache.pulsar.protocols.grpc.api.CommandLookupTopicResponse;
import org.apache.pulsar.protocols.grpc.api.CommandMessage;
import org.apache.pulsar.protocols.grpc.api.CommandNewTxn;
import org.apache.pulsar.protocols.grpc.api.CommandNewTxnResponse;
import org.apache.pulsar.protocols.grpc.api.CommandPartitionedTopicMetadata;
import org.apache.pulsar.protocols.grpc.api.CommandPartitionedTopicMetadataResponse;
import org.apache.pulsar.protocols.grpc.api.CommandProducer;
import org.apache.pulsar.protocols.grpc.api.CommandProducerSuccess;
import org.apache.pulsar.protocols.grpc.api.CommandReachedEndOfTopic;
import org.apache.pulsar.protocols.grpc.api.CommandRedeliverUnacknowledgedMessages;
import org.apache.pulsar.protocols.grpc.api.CommandSeek;
import org.apache.pulsar.protocols.grpc.api.CommandSend;
import org.apache.pulsar.protocols.grpc.api.CommandSendError;
import org.apache.pulsar.protocols.grpc.api.CommandSendReceipt;
import org.apache.pulsar.protocols.grpc.api.CommandSubscribe;
import org.apache.pulsar.protocols.grpc.api.CommandSubscribe.InitialPosition;
import org.apache.pulsar.protocols.grpc.api.CommandSubscribe.SubType;
import org.apache.pulsar.protocols.grpc.api.CommandSubscribeSuccess;
import org.apache.pulsar.protocols.grpc.api.CommandSuccess;
import org.apache.pulsar.protocols.grpc.api.CommandUnsubscribe;
import org.apache.pulsar.protocols.grpc.api.CompressionType;
import org.apache.pulsar.protocols.grpc.api.ConsumeInput;
import org.apache.pulsar.protocols.grpc.api.ConsumeOutput;
import org.apache.pulsar.protocols.grpc.api.EncryptionKeys;
import org.apache.pulsar.protocols.grpc.api.IntRange;
import org.apache.pulsar.protocols.grpc.api.KeySharedMeta;
import org.apache.pulsar.protocols.grpc.api.KeySharedMode;
import org.apache.pulsar.protocols.grpc.api.KeyValue;
import org.apache.pulsar.protocols.grpc.api.MessageIdData;
import org.apache.pulsar.protocols.grpc.api.MessageMetadata;
import org.apache.pulsar.protocols.grpc.api.Messages;
import org.apache.pulsar.protocols.grpc.api.MetadataAndPayload;
import org.apache.pulsar.protocols.grpc.api.PayloadType;
import org.apache.pulsar.protocols.grpc.api.PulsarGrpc;
import org.apache.pulsar.protocols.grpc.api.Schema;
import org.apache.pulsar.protocols.grpc.api.SendResult;
import org.apache.pulsar.protocols.grpc.api.ServerError;
import org.apache.pulsar.protocols.grpc.api.SingleMessage;
import org.apache.pulsar.protocols.grpc.api.SingleMessageMetadata;
import org.apache.pulsar.protocols.grpc.api.TxnAction;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.pulsar.common.protocol.Commands.serializeMetadataAndPayload;
import static org.apache.pulsar.protocols.grpc.Constants.CONSUMER_PARAMS_METADATA_KEY;
import static org.apache.pulsar.protocols.grpc.Constants.ERROR_CODE_METADATA_KEY;
import static org.apache.pulsar.protocols.grpc.Constants.PRODUCER_PARAMS_METADATA_KEY;

public class Commands {

    public static final short MAGIC_CRC_32_C = 0x0e01;

    public static StatusRuntimeException newStatusException(Status status, String message, Throwable exception, ServerError code) {
        Metadata metadata  = new Metadata();
        metadata.put(ERROR_CODE_METADATA_KEY, String.valueOf(code.getNumber()));
        return status.withDescription(message)
                .withCause(exception)
                .asRuntimeException(metadata);
    }

    public static StatusRuntimeException newStatusException(Status status, Throwable exception, ServerError code) {
        return newStatusException(status, exception.getMessage(), exception, code);
    }

    public static CommandAuthChallenge newAuthChallenge(String authMethod, org.apache.pulsar.common.api.AuthData brokerData, long stateId) {
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
        return newSend(sequenceId, numMessages, 0, 0, messageMetadata, payload);
    }

    public static CommandSend newSend(long lowestSequenceId, long highestSequenceId, int numMessages,
            PulsarApi.MessageMetadata messageMetadata, ByteBuf payload) {
        return newSend(lowestSequenceId, highestSequenceId, numMessages, 0, 0,
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
        CommandLookupTopic.Builder lookupTopicBuilder = CommandLookupTopic.newBuilder();
        lookupTopicBuilder.setTopic(topic);
        lookupTopicBuilder.setAuthoritative(authoritative);
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
            SchemaInfo schemaInfo, boolean createTopicIfDoesNotExist, KeySharedPolicy keySharedPolicy, PayloadType payloadType) {
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
            switch (keySharedPolicy.getKeySharedMode()) {
                case AUTO_SPLIT:
                    subscribeBuilder.setKeySharedMeta(KeySharedMeta.newBuilder()
                            .setKeySharedMode(KeySharedMode.AUTO_SPLIT));
                    break;
                case STICKY:
                    KeySharedMeta.Builder builder = KeySharedMeta.newBuilder()
                            .setKeySharedMode(KeySharedMode.STICKY);
                    List<Range> ranges = ((KeySharedPolicy.KeySharedPolicySticky) keySharedPolicy)
                            .getRanges();
                    for (Range range : ranges) {
                        builder.addHashRanges(IntRange.newBuilder()
                                .setStart(range.getStart())
                                .setEnd(range.getEnd()));
                    }
                    subscribeBuilder.setKeySharedMeta(builder);
                    break;
            }
        }

        if (startMessageId != null) {
            subscribeBuilder.setStartMessageId(startMessageId);
        }
        if (startMessageRollbackDurationInSec > 0) {
            subscribeBuilder.setStartMessageRollbackDurationSec(startMessageRollbackDurationInSec);
        }
        subscribeBuilder.putAllMetadata(metadata);

        Schema schema = null;
        if (schemaInfo != null) {
            schema = getSchema(schemaInfo);
            subscribeBuilder.setSchema(schema);
        }

        if (payloadType != null) {
            subscribeBuilder.setPreferedPayloadType(payloadType);
        }

        return subscribeBuilder.build();
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
        return newAck(messageIdData.getLedgerId(), messageIdData.getEntryId(), ackType, null,
                Collections.emptyMap(), 0, 0);
    }

    public static ConsumeInput newAck(long ledgerId, long entryId, CommandAck.AckType ackType,
            CommandAck.ValidationError validationError, Map<String, Long> properties) {
        return newAck(ledgerId, entryId, ackType, validationError, properties, 0, 0);
    }

    public static ConsumeInput newAck(long ledgerId, long entryId, CommandAck.AckType ackType,
            CommandAck.ValidationError validationError, Map<String, Long> properties, long txnIdLeastBits,
            long txnIdMostBits) {
        CommandAck.Builder ackBuilder = CommandAck.newBuilder();
        ackBuilder.setAckType(ackType);
        MessageIdData.Builder messageIdDataBuilder = MessageIdData.newBuilder();
        messageIdDataBuilder.setLedgerId(ledgerId);
        messageIdDataBuilder.setEntryId(entryId);
        MessageIdData messageIdData = messageIdDataBuilder.build();
        ackBuilder.addMessageId(messageIdData);
        if (validationError != null) {
            ackBuilder.setValidationError(validationError);
        }
        if (txnIdMostBits > 0) {
            ackBuilder.setTxnidMostBits(txnIdMostBits);
        }
        if (txnIdLeastBits > 0) {
            ackBuilder.setTxnidLeastBits(txnIdLeastBits);
        }
        ackBuilder.putAllProperties(properties);
        return ConsumeInput.newBuilder().setAck(ackBuilder).build();
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
            ByteBuf metadataAndPayload, PayloadType preferedPayloadType) throws IOException {
        CommandMessage.Builder msgBuilder = CommandMessage.newBuilder();
        msgBuilder.setMessageId(messageIdBuilder);
        if (redeliveryCount > 0) {
            msgBuilder.setRedeliveryCount(redeliveryCount);
        }
        if (preferedPayloadType == PayloadType.BINARY) {
            ByteString headersAndPayload = ByteString.copyFrom(metadataAndPayload.nioBuffer());
            msgBuilder.setBinaryMetadataAndPayload(headersAndPayload);
        } else {
            // TODO: parse directly to gRPC's MessageMetadata (see SingleMessageMetadata for reference)
            MessageMetadata metadata = parseMessageMetadata(metadataAndPayload);
            ByteBuf uncompressedPayload = metadataAndPayload;
            if (preferedPayloadType != PayloadType.METADATA_AND_PAYLOAD
                    && metadata.getCompression() != CompressionType.NONE
                    && metadata.getEncryptionKeysCount() == 0) {
                CompressionCodec compressor = CompressionCodecProvider.getCompressionCodec(convertCompressionType(metadata.getCompression()));
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
            //metadata.recycle();
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
        ByteString singleMessagePayload = ByteString.copyFrom(uncompressedPayload.nioBuffer(readerIndex, singleMessagePayloadSize));
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
        CommandRedeliverUnacknowledgedMessages redeliver = redeliverBuilder.build();
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

    public static ConsumeInput newSeek(long requestId, long ledgerId, long entryId) {
        CommandSeek.Builder seekBuilder = CommandSeek.newBuilder();
        seekBuilder.setRequestId(requestId);

        MessageIdData.Builder messageIdBuilder = MessageIdData.newBuilder();
        messageIdBuilder.setLedgerId(ledgerId);
        messageIdBuilder.setEntryId(entryId);
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

    public static CommandGetTopicsOfNamespace newGetTopicsOfNamespaceRequest(String namespace, CommandGetTopicsOfNamespace.Mode mode) {
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
        return  CommandAddPartitionToTxn.newBuilder()
                .setTxnidLeastBits(txnIdLeastBits)
                .setTxnidMostBits(txnIdMostBits)
                .addAllPartitions(partitions)
                .build();
    }

    public static CommandAddPartitionToTxnResponse newAddPartitionToTxnResponse() {
        return CommandAddPartitionToTxnResponse.getDefaultInstance();
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
                .setTxnidMostBits(txnIdMostBits)
                .build();
    }

    public static PulsarGrpc.PulsarStub attachProducerParams(PulsarGrpc.PulsarStub stub, CommandProducer producerParams) {
        Metadata headers = new Metadata();
        headers.put(PRODUCER_PARAMS_METADATA_KEY, producerParams.toByteArray());
        return MetadataUtils.attachHeaders(stub, headers);
    }

    public static PulsarGrpc.PulsarStub attachConsumerParams(PulsarGrpc.PulsarStub stub, CommandSubscribe consumerParams) {
        Metadata headers = new Metadata();
        headers.put(CONSUMER_PARAMS_METADATA_KEY, consumerParams.toByteArray());
        return MetadataUtils.attachHeaders(stub, headers);
    }

    public static ServerError convertServerError(PulsarApi.ServerError serverError) {
        if(serverError == null) {
            return null;
        }
        switch(serverError) {
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
            case UnknownError:
            default:
                return ServerError.UnknownError;
        }
    }

    public static PulsarApi.CommandSubscribe.SubType convertSubscribeSubType(SubType subType) {
        if(subType == null) {
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

    public static PulsarApi.CommandSubscribe.InitialPosition convertSubscribeInitialPosition(InitialPosition initialPosition) {
        if(initialPosition == null) {
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
        if(mode == null) {
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
        PulsarApi.MessageIdData messageIdData_ = builder.build();
        builder.recycle();
        return messageIdData_;
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
        ack.getPropertiesMap().forEach((k,v) -> {
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
        PulsarApi.CommandAck ack_ = builder.build();
        builder.recycle();
        return ack_;
    }

    public static PulsarApi.CommandGetTopicsOfNamespace.Mode convertGetTopicsOfNamespaceMode(CommandGetTopicsOfNamespace.Mode mode) {
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

    public static PulsarApi.SingleMessageMetadata.Builder convertSingleMessageMetadata(SingleMessageMetadata messageMetadata) {
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
            builder.setOrderingKey(org.apache.pulsar.shaded.com.google.protobuf.v241.ByteString.copyFrom(messageMetadata.getOrderingKey().asReadOnlyByteBuffer()));
        }
        if (messageMetadata.hasSequenceId()) {
            builder.setSequenceId(messageMetadata.getSequenceId());
        }
        if (messageMetadata.hasNullValue()) {
            builder.setNullValue(messageMetadata.getNullValue());
        }
        messageMetadata.getPropertiesMap().forEach(
                (k,v) -> builder.addProperties(PulsarApi.KeyValue.newBuilder().setKey(k).setValue(v))
        );
        return builder;
    }

    public static KeyValue convertKeyValue(PulsarApi.KeyValue keyValue) {
        return KeyValue.newBuilder()
                .setKey(keyValue.getKey())
                .setValue(keyValue.getValue())
                .build();
    }

    public static EncryptionKeys convertEncryptionKeys(PulsarApi.EncryptionKeys keys) {
        EncryptionKeys.Builder builder = EncryptionKeys.newBuilder()
                .setKey(keys.getKey())
                .setValue(ByteString.copyFrom(keys.getValue().asReadOnlyByteBuffer()));
        keys.getMetadataList().forEach(keyValue -> builder.addMetadata(convertKeyValue(keyValue)));
        return builder.build();
    }

    public static MessageMetadata convertMessageMetadata(PulsarApi.MessageMetadata metadata) {
        MessageMetadata.Builder builder = MessageMetadata.newBuilder()
                .setProducerName(metadata.getProducerName())
                .setSequenceId(metadata.getSequenceId())
                .setPublishTime(metadata.getPublishTime());
        metadata.getPropertiesList().forEach(
                property -> builder.addProperties(convertKeyValue(property))
        );
        if (metadata.hasReplicatedFrom()) {
            builder.setReplicatedFrom(metadata.getReplicatedFrom());
        }
        if (metadata.hasPartitionKey()) {
            builder.setPartitionKey(metadata.getPartitionKey());
        }
        metadata.getReplicateToList().forEach(builder::addReplicateTo);
        if (metadata.hasCompression()) {
            builder.setCompression(convertCompressionType(metadata.getCompression()));
        }
        if (metadata.hasUncompressedSize()) {
            builder.setUncompressedSize(metadata.getUncompressedSize());
        }
        if (metadata.hasNumMessagesInBatch()) {
            builder.setNumMessagesInBatch(metadata.getNumMessagesInBatch());
        }
        if (metadata.hasEventTime()) {
            builder.setEventTime(metadata.getEventTime());
        }
        metadata.getEncryptionKeysList().forEach(
                key -> builder.addEncryptionKeys(convertEncryptionKeys(key))
        );
        if (metadata.hasEncryptionAlgo()) {
            builder.setEncryptionAlgo(metadata.getEncryptionAlgo());
        }
        if (metadata.hasEncryptionParam()) {
            builder.setEncryptionParam(ByteString.copyFrom(metadata.getEncryptionParam().asReadOnlyByteBuffer()));
        }
        if (metadata.hasSchemaVersion()) {
            builder.setSchemaVersion(ByteString.copyFrom(metadata.getSchemaVersion().asReadOnlyByteBuffer()));
        }
        if (metadata.hasPartitionKeyB64Encoded()) {
            builder.setPartitionKeyB64Encoded(metadata.getPartitionKeyB64Encoded());
        }
        if (metadata.hasOrderingKey()) {
            builder.setOrderingKey(ByteString.copyFrom(metadata.getOrderingKey().asReadOnlyByteBuffer()));
        }
        if (metadata.hasDeliverAtTime()) {
            builder.setDeliverAtTime(metadata.getDeliverAtTime());
        }
        if (metadata.hasMarkerType()) {
            builder.setMarkerType(metadata.getMarkerType());
        }
        if (metadata.hasTxnidLeastBits()) {
            builder.setTxnidLeastBits(metadata.getTxnidLeastBits());
        }
        if (metadata.hasTxnidMostBits()) {
            builder.setTxnidMostBits(metadata.getTxnidMostBits());
        }
        if (metadata.hasHighestSequenceId()) {
            builder.setHighestSequenceId(metadata.getHighestSequenceId());
        }
        if (metadata.hasNullValue()) {
            builder.setNullValue(metadata.getNullValue());
        }
        return builder.build();
    }
}
