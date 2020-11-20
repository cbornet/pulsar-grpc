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

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.CodedOutputStream;
import com.scurrilous.circe.checksum.Crc32cIntChecksum;
import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.EventLoop;
import org.apache.bookkeeper.mledger.util.SafeRun;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.PulsarCommandSender;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.compression.CompressionCodec;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.protocols.grpc.api.CommandSend;
import org.apache.pulsar.protocols.grpc.api.CompressionType;
import org.apache.pulsar.protocols.grpc.api.MessageMetadata;
import org.apache.pulsar.protocols.grpc.api.Messages;
import org.apache.pulsar.protocols.grpc.api.MetadataAndPayload;
import org.apache.pulsar.protocols.grpc.api.SendResult;
import org.apache.pulsar.protocols.grpc.api.SingleMessage;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static org.apache.pulsar.common.protocol.Commands.serializeSingleMessageInBatchWithPayload;
import static org.apache.pulsar.protocols.grpc.Commands.convertCompressionType;
import static org.apache.pulsar.protocols.grpc.Commands.convertSingleMessageMetadata;

public class ProducerCnx extends AbstractGrpcCnx {
    private final CallStreamObserver<SendResult> responseObserver;
    private final ProducerCommandSender producerCommandSender;
    private final EventLoop eventLoop;
    private final boolean preciseDispatcherFlowControl;
    private boolean preciseTopicPublishRateLimitingEnable;

    // Max number of pending requests per produce RPC
    private final int maxPendingSendRequests;
    private final int resumeReadsThreshold;
    private int pendingSendRequest = 0;
    private int nonPersistentPendingMessages = 0;
    private final int MaxNonPersistentPendingMessages;
    private volatile boolean isAutoRead = true;
    private volatile boolean autoReadDisabledRateLimiting = false;
    private final AutoReadAwareOnReadyHandler onReadyHandler = new AutoReadAwareOnReadyHandler();
    // Flag to manage throttling-publish-buffer by atomically enable/disable read-channel.
    private volatile boolean autoReadDisabledPublishBufferLimiting = false;
    private static final AtomicLongFieldUpdater<ProducerCnx> MSG_PUBLISH_BUFFER_SIZE_UPDATER =
            AtomicLongFieldUpdater.newUpdater(ProducerCnx.class, "messagePublishBufferSize");
    private volatile long messagePublishBufferSize = 0;

    public ProducerCnx(BrokerService service, SocketAddress remoteAddress, String authRole,
            AuthenticationDataSource authenticationData, StreamObserver<SendResult> responseObserver,
            EventLoop eventLoop) {
        super(service, remoteAddress, authRole, authenticationData);
        this.MaxNonPersistentPendingMessages = service.pulsar().getConfiguration()
                .getMaxConcurrentNonPersistentMessagePerConnection();
        this.maxPendingSendRequests = service.pulsar().getConfiguration().getMaxPendingPublishRequestsPerConnection();
        this.resumeReadsThreshold = maxPendingSendRequests / 2;
        this.preciseDispatcherFlowControl = service.pulsar().getConfiguration().isPreciseDispatcherFlowControl();
        this.preciseTopicPublishRateLimitingEnable = service.pulsar().getConfiguration().isPreciseTopicPublishRateLimiterEnable();
        this.responseObserver = (CallStreamObserver<SendResult>) responseObserver;
        this.responseObserver.disableAutoInboundFlowControl();
        this.responseObserver.setOnReadyHandler(onReadyHandler);

        this.producerCommandSender = new ProducerCommandSender(responseObserver);
        this.eventLoop = eventLoop;
    }

    @Override
    public PulsarCommandSender getCommandSender() {
        return producerCommandSender;
    }

    @Override
    public void closeProducer(Producer producer) {
        responseObserver.onCompleted();
    }

    @Override
    public void disableCnxAutoRead() {
        isAutoRead = false;
    }

    public void handleSend(CommandSend send, Producer producer) {
        ByteBuf headersAndPayload;
        int numMessages = send.getNumMessages();
        long sequenceId = send.getSequenceId();
        Long highestSequenceId = send.hasHighestSequenceId() ? send.getHighestSequenceId() : null;

        switch (send.getSendOneofCase()) {
            case BINARY_METADATA_AND_PAYLOAD:
                if(!send.hasSequenceId()) {
                    return;
                }
                ByteBuffer buffer = send.getBinaryMetadataAndPayload().asReadOnlyByteBuffer();
                headersAndPayload = Unpooled.wrappedBuffer(buffer);
                break;
            case MESSAGES:
                Messages sendMessages = send.getMessages();
                MessageMetadata metadata = sendMessages.getMetadata();
                if (!send.hasSequenceId()) {
                    sequenceId = metadata.getSequenceId();
                }
                if (highestSequenceId == null && metadata.hasHighestSequenceId()) {
                    highestSequenceId = metadata.getHighestSequenceId();
                }
                List<SingleMessage> messages = sendMessages.getMessagesList();
                numMessages = messages.size();
                if (numMessages == 0) {
                    // No message!
                    return;
                }
                MessageMetadata.Builder metadataBuilder = metadata.toBuilder();
                metadataBuilder.setNumMessagesInBatch(messages.size());
                ByteBuf batchedMessageMetadataAndPayload = PulsarByteBufAllocator.DEFAULT.buffer(1024);
                for (SingleMessage message : messages) {
                    PulsarApi.SingleMessageMetadata.Builder singleMessageMetadata = convertSingleMessageMetadata(message.getMetadata());
                    ByteBuf payload = Unpooled.wrappedBuffer(message.getPayload().asReadOnlyByteBuffer());
                    try {
                        serializeSingleMessageInBatchWithPayload(singleMessageMetadata, payload, batchedMessageMetadataAndPayload);
                    } finally {
                        singleMessageMetadata.recycle();
                    }
                }
                headersAndPayload = compressAndSerialize(metadataBuilder, batchedMessageMetadataAndPayload);
                break;
            case METADATA_AND_PAYLOAD:
                MetadataAndPayload metadataAndPayload = send.getMetadataAndPayload();
                metadata = metadataAndPayload.getMetadata();
                if (!send.hasNumMessages() && metadata.hasNumMessagesInBatch()) {
                    numMessages = metadata.getNumMessagesInBatch();
                }
                // Since https://github.com/apache/pulsar/pull/5390 Pulsar uses the airlift lib to compress which doesn't support
                // ReadOnlyByteBuffer as input. So we have to make a copy here...
                // ByteBuf payload = Unpooled.wrappedBuffer(metadataAndPayload.getPayload().asReadOnlyByteBuffer());
                // TODO: check if JNI compression could be used instead of airlift ?
                ByteBuf payload = Unpooled.wrappedBuffer(metadataAndPayload.getPayload().toByteArray());
                if (metadataAndPayload.getCompress() && metadata.getCompression() != CompressionType.NONE) {
                    headersAndPayload = compressAndSerialize(metadata.toBuilder(), payload);
                } else {
                    headersAndPayload = serializeMetadataAndPayload(metadata, payload);
                }
                break;
            case SENDONEOF_NOT_SET:
            default:
                return;
        }

        if (producer.isNonPersistentTopic()) {
            // avoid processing non-persist message if reached max concurrent-message limit
            if (nonPersistentPendingMessages > MaxNonPersistentPendingMessages) {
                final long sequenceId_ = sequenceId;
                final long highestSequenceId_ = highestSequenceId == null ? 0 : highestSequenceId;
                service.getTopicOrderedExecutor().executeOrdered(
                        producer.getTopic().getName(),
                        SafeRun.safeRun(() -> responseObserver.onNext(Commands.newSendReceipt(sequenceId_, highestSequenceId_, -1, -1)))
                );
                producer.recordMessageDrop(numMessages);
                return;
            } else {
                nonPersistentPendingMessages++;
            }
        }

        startSendOperation(producer, headersAndPayload.readableBytes(), send.getNumMessages());

        // Persist the message
        if (highestSequenceId != null && sequenceId <= highestSequenceId) {
            producer.publishMessage(producer.getProducerId(), sequenceId, highestSequenceId,
                    headersAndPayload, numMessages, send.getIsChunk());
        } else {
            producer.publishMessage(producer.getProducerId(), sequenceId, headersAndPayload,
                    numMessages, send.getIsChunk());
        }
        onMessageHandled();
    }

    private static ByteBuf compressAndSerialize(MessageMetadata.Builder metadataBuilder, ByteBuf payload) {
        ByteBuf headersAndPayload;
        if (metadataBuilder.getCompression() != CompressionType.NONE) {
            PulsarApi.CompressionType compressionType = convertCompressionType(metadataBuilder.getCompression());
            CompressionCodec compressor = CompressionCodecProvider.getCompressionCodec(compressionType);
            int uncompressedSize = payload.readableBytes();
            ByteBuf compressedPayload = compressor.encode(payload);
            payload.release();
            metadataBuilder.setUncompressedSize(uncompressedSize);
            try {
                headersAndPayload = serializeMetadataAndPayload(metadataBuilder.build(), compressedPayload);
            } finally {
                compressedPayload.release();
            }
        } else {
            headersAndPayload = serializeMetadataAndPayload(metadataBuilder.build(), payload);
        }
        return headersAndPayload;
    }

    private void startSendOperation(Producer producer, int msgSize, int numMessages) {
        MSG_PUBLISH_BUFFER_SIZE_UPDATER.getAndAdd(this, msgSize);
        boolean isPublishRateExceeded = false;
        if (preciseTopicPublishRateLimitingEnable) {
            boolean isPreciseTopicPublishRateExceeded = producer.getTopic().isTopicPublishRateExceeded(numMessages, msgSize);
            if (isPreciseTopicPublishRateExceeded) {
                producer.getTopic().disableCnxAutoRead();
                return;
            }
            isPublishRateExceeded = producer.getTopic().isBrokerPublishRateExceeded();
        } else {
            isPublishRateExceeded = producer.getTopic().isPublishRateExceeded();
        }

        if (++pendingSendRequest == maxPendingSendRequests || isPublishRateExceeded) {
            // When the quota of pending send requests is reached, stop reading from channel to cause backpressure on
            // client connection
            isAutoRead = false;
            autoReadDisabledRateLimiting = isPublishRateExceeded;
        }

        if (getBrokerService().isReachMessagePublishBufferThreshold()) {
            isAutoRead = false;
            autoReadDisabledPublishBufferLimiting = true;
        }
    }

    @Override
    public boolean isPreciseDispatcherFlowControl() {
        return preciseDispatcherFlowControl;
    }

    @Override
    public void completedSendOperation(boolean isNonPersistentTopic, int msgSize) {
        MSG_PUBLISH_BUFFER_SIZE_UPDATER.getAndAdd(this, -msgSize);
        if (--pendingSendRequest == resumeReadsThreshold) {
            // Resume producer
            isAutoRead = true;
            if (responseObserver.isReady()) {
                responseObserver.request(1);
            }
        }
        if (isNonPersistentTopic) {
            nonPersistentPendingMessages--;
        }
    }

    @Override
    public void enableCnxAutoRead() {
        // we can add check (&& pendingSendRequest < maxPendingSendRequests) here but then it requires
        // pendingSendRequest to be volatile and it can be expensive while writing. also this will be called on if
        // throttling is enable on the topic. so, avoid pendingSendRequest check will be fine.
        if (!isAutoRead && !autoReadDisabledRateLimiting && !autoReadDisabledPublishBufferLimiting) {
            // Resume reading from socket if pending-request is not reached to threshold
            isAutoRead = true;
            // triggers channel read
            if (responseObserver.isReady()) {
                responseObserver.request(1);
            }
        }
    }

    @VisibleForTesting
    @Override
    public void cancelPublishRateLimiting() {
        if (autoReadDisabledRateLimiting) {
            autoReadDisabledRateLimiting = false;
        }
    }

    @VisibleForTesting
    @Override
    public void cancelPublishBufferLimiting() {
        if (autoReadDisabledPublishBufferLimiting) {
            autoReadDisabledPublishBufferLimiting = false;
        }
    }

    @Override
    public long getMessagePublishBufferSize() {
        return this.messagePublishBufferSize;
    }

    @VisibleForTesting
    void setMessagePublishBufferSize(long bufferSize) {
        this.messagePublishBufferSize = bufferSize;
    }

    @VisibleForTesting
    void setAutoReadDisabledRateLimiting(boolean isLimiting) {
        this.autoReadDisabledRateLimiting = isLimiting;
    }

    public void onMessageHandled() {
        if (responseObserver.isReady() && isAutoRead) {
            responseObserver.request(1);
        } else {
            onReadyHandler.wasReady = false;
        }
    }

    class AutoReadAwareOnReadyHandler implements Runnable {
        // Guard against spurious onReady() calls caused by a race between onNext() and onReady(). If the transport
        // toggles isReady() from false to true while onNext() is executing, but before onNext() checks isReady(),
        // request(1) would be called twice - once by onNext() and once by the onReady() scheduled during onNext()'s
        // execution.
        private boolean wasReady = false;

        @Override
        public void run() {
            if (responseObserver.isReady() && !wasReady) {
                wasReady = true;
                if(isAutoRead) {
                    responseObserver.request(1);
                }
            }
        }

    }

    @Override
    public void execute(Runnable runnable) {
        eventLoop.execute(runnable);
    }

    private static ByteBuf serializeMetadataAndPayload(MessageMetadata msgMetadata, ByteBuf payload) {
        int msgMetadataSize = msgMetadata.getSerializedSize();
        int payloadSize = payload.readableBytes();
        int headerContentSize = 10 + msgMetadataSize;
        int checksumReaderIndex;
        int totalSize = headerContentSize + payloadSize;
        ByteBuf metadataAndPayload = PulsarByteBufAllocator.DEFAULT.buffer(totalSize, totalSize);

        try {
            metadataAndPayload.writeShort(3585);
            checksumReaderIndex = metadataAndPayload.writerIndex();
            metadataAndPayload.writerIndex(metadataAndPayload.writerIndex() + 4);
            metadataAndPayload.writeInt(msgMetadataSize);
            CodedOutputStream outStream = CodedOutputStream.newInstance(
                    metadataAndPayload.nioBuffer(metadataAndPayload.writerIndex(), metadataAndPayload.writableBytes()));
            msgMetadata.writeTo(outStream);
            metadataAndPayload.writerIndex(metadataAndPayload.writerIndex() + msgMetadataSize);
        } catch (IOException var13) {
            throw new RuntimeException(var13);
        }

        metadataAndPayload.markReaderIndex();
        metadataAndPayload.readerIndex(checksumReaderIndex + 4);
        int metadataChecksum = Crc32cIntChecksum.computeChecksum(metadataAndPayload);
        int computedChecksum = Crc32cIntChecksum.resumeChecksum(metadataChecksum, payload);
        metadataAndPayload.setInt(checksumReaderIndex, computedChecksum);
        metadataAndPayload.resetReaderIndex();

        metadataAndPayload.writeBytes(payload);
        return metadataAndPayload;
    }
}
