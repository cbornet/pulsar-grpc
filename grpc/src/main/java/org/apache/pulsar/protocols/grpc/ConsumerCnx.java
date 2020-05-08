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

import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.EntryBatchSizes;
import org.apache.pulsar.broker.service.RedeliveryTracker;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.protocols.grpc.api.ConsumeOutput;
import org.apache.pulsar.protocols.grpc.api.MessageIdData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class ConsumerCnx implements ServerCnx {

    private static final Logger log = LoggerFactory.getLogger(ConsumerCnx.class);

    private final BrokerService service;
    private final SocketAddress remoteAddress;
    private final String authRole;
    private final AuthenticationDataSource authenticationData;
    private final StreamObserver<ConsumeOutput> responseObserver;

    public ConsumerCnx(BrokerService service, SocketAddress remoteAddress, String authRole,
            AuthenticationDataSource authenticationData, StreamObserver<ConsumeOutput> responseObserver) {
        this.service = service;
        this.remoteAddress = remoteAddress;
        this.authRole = authRole;
        this.authenticationData = authenticationData;
        this.responseObserver = responseObserver;
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
    public void closeConsumer(Consumer consumer) {
        responseObserver.onCompleted();
    }

    @Override
    public void sendActiveConsumerChange(long consumerId, boolean isActive) {
        responseObserver.onNext(Commands.newActiveConsumerChange(isActive));
    }

    @Override
    public void sendSuccess(long requestId) {
        responseObserver.onNext(Commands.newSuccess(requestId));
    }

    @Override
    public void sendError(long requestId, PulsarApi.ServerError error, String message) {
        responseObserver.onNext(Commands.newError(requestId, Commands.convertServerError(error), message));
    }

    @Override
    public void sendReachedEndOfTopic(long consumerId) {
        responseObserver.onNext(Commands.newReachedEndOfTopic());
    }

    @Override
    public CompletableFuture<Void> sendMessagesToConsumer(long consumerId, String topicName, Subscription subscription,
            int partitionIdx, List<Entry> entries, EntryBatchSizes batchSizes, RedeliveryTracker redeliveryTracker) {
        CompletableFuture<Void> writeFuture = new CompletableFuture<>();
        //ctx.channel().eventLoop().execute(() -> {
            for (int i = 0; i < entries.size(); i++) {
                Entry entry = entries.get(i);
                if (entry == null) {
                    // Entry was filtered out
                    continue;
                }

                MessageIdData.Builder messageIdBuilder = MessageIdData.newBuilder();
                MessageIdData messageId = messageIdBuilder
                        .setLedgerId(entry.getLedgerId())
                        .setEntryId(entry.getEntryId())
                        .setPartition(partitionIdx)
                        .build();

                ByteBuf metadataAndPayload = entry.getDataBuffer();
                // increment ref-count of data and release at the end of process: so, we can get chance to call entry.release
                metadataAndPayload.retain();
                // skip checksum by incrementing reader-index if consumer-client doesn't support checksum verification
                //if (getRemoteEndpointProtocolVersion() < PulsarApi.ProtocolVersion.v11.getNumber()) {
                //    org.apache.pulsar.common.protocol.Commands.skipChecksumIfPresent(metadataAndPayload);
               // }

                if (log.isDebugEnabled()) {
                    log.debug("[{}-{}] Sending message to consumer, msg id {}-{}", topicName, subscription, entry.getLedgerId(), entry.getEntryId());
                }

                int redeliveryCount = 0;
                PositionImpl position = PositionImpl.get(messageId.getLedgerId(), messageId.getEntryId());
                if (redeliveryTracker.contains(position)) {
                    redeliveryCount = redeliveryTracker.incrementAndGetRedeliveryCount(position);
                }

                responseObserver.onNext(Commands.newMessage(messageId, redeliveryCount, metadataAndPayload));
                //messageId.recycle();
                //messageIdBuilder.recycle();
                entry.release();
            }

            /*final ChannelPromise writePromise = ctx.newPromise().addListener(future -> {
                if(future.isSuccess()) {
                    writeFuture.complete(null);
                } else {
                    writeFuture.completeExceptionally(future.cause());
                }
            });*/
            // Use an empty write here so that we can just tie the flush with the write promise for last entry
            //ctx.writeAndFlush(Unpooled.EMPTY_BUFFER, writePromise);
            batchSizes.recyle();
        //});
        writeFuture.complete(null);
            return writeFuture;

    }
}
