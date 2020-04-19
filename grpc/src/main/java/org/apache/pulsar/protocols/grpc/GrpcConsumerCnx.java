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

import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.EventLoop;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.util.SafeRun;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.service.*;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.protocols.grpc.api.CommandSend;
import org.apache.pulsar.protocols.grpc.api.ConsumeOutput;
import org.apache.pulsar.protocols.grpc.api.SendResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class GrpcConsumerCnx implements ServerCnx {
    private final BrokerService service;
    private final SocketAddress remoteAddress;
    private final String authRole;
    private final AuthenticationDataSource authenticationData;
    private final StreamObserver<ConsumeOutput> responseObserver;

    public GrpcConsumerCnx(BrokerService service, SocketAddress remoteAddress, String authRole,
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
    public void removedConsumer(Consumer consumer) {
        responseObserver.onCompleted();
    }

    @Override
    public void closeConsumer(Consumer consumer) {
        responseObserver.onCompleted();
    }

    @Override
    public void sendActiveConsumerChange(long consumerId, boolean isActive) {
    }

    @Override
    public void sendSuccess(long requestId) {
    }

    @Override
    public void sendError(long requestId, PulsarApi.ServerError error, String message) {
    }

    @Override
    public void sendReachedEndOfTopic(long consumerId) {
    }

    @Override
    public CompletableFuture<Void> sendMessagesToConsumer(long consumerId, String topicName, Subscription subscription, int partitionIdx, List<Entry> entries, EntryBatchSizes batchSizes, RedeliveryTracker redeliveryTracker) {
        return null;
    }
}
