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

import io.grpc.stub.StreamObserver;
import org.apache.pulsar.protocols.grpc.api.SendResult;

class ProducerCommandSender extends DefaultGrpcCommandSender {

    private final StreamObserver<SendResult> responseObserver;

    public ProducerCommandSender(StreamObserver<SendResult> responseObserver) {
        this.responseObserver = responseObserver;
    }

    @Override
    public void sendSendError(long producerId, long sequenceId,
            org.apache.pulsar.common.api.proto.PulsarApi.ServerError serverError, String message) {
        responseObserver.onNext(Commands.newSendError(sequenceId, Commands.convertServerError(serverError), message));
    }

    @Override
    public void sendSendReceiptResponse(long producerId, long sequenceId, long highestSequenceId, long ledgerId,
            long entryId) {
        responseObserver.onNext(Commands.newSendReceipt(sequenceId, highestSequenceId, ledgerId, entryId));
    }

}
