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
import org.apache.pulsar.broker.service.ServerCnx;
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

import static org.apache.pulsar.common.protocol.Commands.serializeSingleMessageInBatchWithPayload;
import static org.apache.pulsar.protocols.grpc.Commands.convertCompressionType;
import static org.apache.pulsar.protocols.grpc.Commands.convertSingleMessageMetadata;

public class ProducerCnx implements ServerCnx {
    private final BrokerService service;
    private final SocketAddress remoteAddress;
    private final String authRole;
    private final AuthenticationDataSource authenticationData;
    private final CallStreamObserver<SendResult> responseObserver;
    private final EventLoop eventLoop;

    // Max number of pending requests per produce RPC
    private static final int MaxPendingSendRequests = 1000;
    private static final int ResumeReadsThreshold = MaxPendingSendRequests / 2;
    private int pendingSendRequest = 0;
    private int nonPersistentPendingMessages = 0;
    private final int MaxNonPersistentPendingMessages;
    private volatile boolean isAutoRead = true;
    private volatile boolean autoReadDisabledRateLimiting = false;
    private final AutoReadAwareOnReadyHandler onReadyHandler = new AutoReadAwareOnReadyHandler();

    public ProducerCnx(BrokerService service, SocketAddress remoteAddress, String authRole,
            AuthenticationDataSource authenticationData, StreamObserver<SendResult> responseObserver,
            EventLoop eventLoop) {
        this.service = service;
        this.remoteAddress = remoteAddress;
        this.MaxNonPersistentPendingMessages = service.pulsar().getConfiguration()
                .getMaxConcurrentNonPersistentMessagePerConnection();
        this.authRole = authRole;
        this.authenticationData = authenticationData;
        this.responseObserver = (CallStreamObserver<SendResult>) responseObserver;
        this.responseObserver.disableAutoInboundFlowControl();
        this.responseObserver.setOnReadyHandler(onReadyHandler);

        this.eventLoop = eventLoop;
    }

    @Override
    public SocketAddress clientAddress() {
        return remoteAddress;
    }

    @Override
    public BrokerService getBrokerService() {
        return service;
    }

    @Override
    public String getRole() {
        return authRole;
    }

    @Override
    public AuthenticationDataSource getAuthenticationData() {
        return authenticationData;
    }

    @Override
    public void closeProducer(Producer producer) {
        responseObserver.onCompleted();
    }

    @Override
    public long getMessagePublishBufferSize() {
        // TODO: implement
        return Long.MAX_VALUE;
    }

    @Override
    public void cancelPublishRateLimiting() {
        // TODO: implement
    }

    @Override
    public void cancelPublishBufferLimiting() {
        // TODO: implement
    }

    @Override
    public void disableCnxAutoRead() {
        // TODO: implement
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
                ByteBuf payload = Unpooled.wrappedBuffer(metadataAndPayload.getPayload().asReadOnlyByteBuffer());
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

        startSendOperation(producer);

        // Persist the message
        if (highestSequenceId != null && sequenceId <= highestSequenceId) {
            producer.publishMessage(producer.getProducerId(), sequenceId, highestSequenceId,
                    headersAndPayload, numMessages);
        } else {
            producer.publishMessage(producer.getProducerId(), sequenceId, headersAndPayload, numMessages);
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

    private void startSendOperation(Producer producer) {
        boolean isPublishRateExceeded = producer.getTopic().isPublishRateExceeded();
        if (++pendingSendRequest == MaxPendingSendRequests || isPublishRateExceeded) {
            // When the quota of pending send requests is reached, stop reading from channel to cause backpressure on
            // client connection
            isAutoRead = false;
            autoReadDisabledRateLimiting = isPublishRateExceeded;
        }
    }

    @Override
    public void completedSendOperation(boolean isNonPersistentTopic, int msgSize) {
        if (--pendingSendRequest == ResumeReadsThreshold) {
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
        // we can add check (&& pendingSendRequest < MaxPendingSendRequests) here but then it requires
        // pendingSendRequest to be volatile and it can be expensive while writing. also this will be called on if
        // throttling is enable on the topic. so, avoid pendingSendRequest check will be fine.
        if (!isAutoRead && autoReadDisabledRateLimiting) {
            // Resume reading from socket if pending-request is not reached to threshold
            isAutoRead = true;
            // triggers channel read
            if (responseObserver.isReady()) {
                responseObserver.request(1);
            }
            autoReadDisabledRateLimiting = false;
        }
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
    public void sendProducerError(long producerId, long sequenceId, org.apache.pulsar.common.api.proto.PulsarApi.ServerError serverError, String message) {
        responseObserver.onNext(Commands.newSendError(sequenceId, Commands.convertServerError(serverError), message));
    }

    @Override
    public void execute(Runnable runnable) {
        eventLoop.execute(runnable);
    }

    @Override
    public void sendProducerReceipt(long producerId, long sequenceId, long highestSequenceId, long ledgerId, long entryId) {
        responseObserver.onNext(Commands.newSendReceipt(sequenceId, highestSequenceId, ledgerId, entryId));
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
