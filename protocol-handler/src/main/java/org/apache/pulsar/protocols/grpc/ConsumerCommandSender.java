/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.protocols.grpc;

import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ImmediateEventExecutor;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.EntryBatchIndexesAcks;
import org.apache.pulsar.broker.service.EntryBatchSizes;
import org.apache.pulsar.broker.service.RedeliveryTracker;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.protocols.grpc.api.ConsumeOutput;
import org.apache.pulsar.protocols.grpc.api.MessageIdData;
import org.apache.pulsar.protocols.grpc.api.PayloadType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

public class ConsumerCommandSender extends DefaultGrpcCommandSender {

    private static final Logger log = LoggerFactory.getLogger(ConsumerCommandSender.class);

    private final StreamObserver<ConsumeOutput> responseObserver;
    private final PayloadType preferedPayloadType;
    private final Consumer<Integer> cb;

    public ConsumerCommandSender(StreamObserver<ConsumeOutput> responseObserver, PayloadType preferedPayloadType,
            Consumer<Integer> cb) {
        this.responseObserver = responseObserver;
        this.preferedPayloadType = preferedPayloadType;
        this.cb = cb;
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
    public Future<Void> sendMessagesToConsumer(long consumerId, String topicName, Subscription subscription,
            int partitionIdx, List<Entry> entries, EntryBatchSizes batchSizes, EntryBatchIndexesAcks batchIndexesAcks,
            RedeliveryTracker redeliveryTracker) {
        for (int i = 0; i < entries.size(); i++) {
            Entry entry = entries.get(i);
            if (entry == null) {
                // Entry was filtered out
                continue;
            }

            MessageIdData.Builder messageIdBuilder = MessageIdData.newBuilder();
            messageIdBuilder
                    .setLedgerId(entry.getLedgerId())
                    .setEntryId(entry.getEntryId())
                    .setPartition(partitionIdx);

            ByteBuf metadataAndPayload = entry.getDataBuffer();

            if (log.isDebugEnabled()) {
                log.debug("[{}-{}] Sending message to consumer, msg id {}-{}", topicName, subscription, entry.getLedgerId(), entry.getEntryId());
            }

            int redeliveryCount = 0;
            PositionImpl position = PositionImpl.get(messageIdBuilder.getLedgerId(), messageIdBuilder.getEntryId());
            if (redeliveryTracker.contains(position)) {
                redeliveryCount = redeliveryTracker.incrementAndGetRedeliveryCount(position);
            }

            try {
                responseObserver.onNext(Commands.newMessage(messageIdBuilder, redeliveryCount, metadataAndPayload,
                        batchIndexesAcks == null ? null : batchIndexesAcks.getAckSet(i), preferedPayloadType));

                cb.accept(1);
            } catch (IOException e) {
                log.error("Couldn't send message", e);
            }

            entry.release();
        }
        batchSizes.recyle();
        // Cannot know when gRPC has effectively sent the message so validate as soon as onNext returns
        // Can we do better ?
        return ImmediateEventExecutor.INSTANCE.newSucceededFuture(null);

    }
}
