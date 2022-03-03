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

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ImmediateEventExecutor;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.broker.service.EntryBatchIndexesAcks;
import org.apache.pulsar.broker.service.EntryBatchSizes;
import org.apache.pulsar.broker.service.PulsarCommandSender;
import org.apache.pulsar.broker.service.RedeliveryTracker;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.common.api.proto.CommandLookupTopicResponse;
import org.apache.pulsar.common.api.proto.ServerError;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.schema.SchemaInfo;

import java.util.List;
import java.util.Optional;

abstract class DefaultGrpcCommandSender implements PulsarCommandSender {

    @Override
    public void sendPartitionMetadataResponse(ServerError error, String errorMsg, long requestId) {

    }

    @Override
    public void sendPartitionMetadataResponse(int partitions, long requestId) {

    }

    @Override
    public void sendSuccessResponse(long requestId) {

    }

    @Override
    public void sendErrorResponse(long requestId, ServerError error, String message) {

    }

    @Override
    public void sendProducerSuccessResponse(long requestId, String producerName, SchemaVersion schemaVersion) {

    }

    @Override
    public void sendProducerSuccessResponse(long requestId, String producerName, long lastSequenceId,
            SchemaVersion schemaVersion, Optional<Long> topicEpoch, boolean isProducerReady) {

    }

    @Override
    public void sendSendReceiptResponse(long producerId, long sequenceId, long highestId, long ledgerId, long entryId) {

    }

    @Override
    public void sendSendError(long producerId, long sequenceId, ServerError error, String errorMsg) {

    }

    @Override
    public void sendGetTopicsOfNamespaceResponse(List<String> topics, long requestId) {

    }

    @Override
    public void sendGetSchemaResponse(long requestId, SchemaInfo schema, SchemaVersion version) {

    }

    @Override
    public void sendGetSchemaErrorResponse(long requestId, ServerError error, String errorMessage) {

    }

    @Override
    public void sendGetOrCreateSchemaResponse(long requestId, SchemaVersion schemaVersion) {

    }

    @Override
    public void sendGetOrCreateSchemaErrorResponse(long requestId, ServerError error, String errorMessage) {

    }

    @Override
    public void sendConnectedResponse(int clientProtocolVersion, int maxMessageSize) {

    }

    @Override
    public void sendLookupResponse(String brokerServiceUrl, String brokerServiceUrlTls, boolean authoritative,
                                   CommandLookupTopicResponse.LookupType response, long requestId,
                                   boolean proxyThroughServiceUrl) {

    }

    @Override
    public void sendLookupResponse(ServerError error, String errorMsg, long requestId) {

    }

    @Override
    public void sendActiveConsumerChange(long consumerId, boolean isActive) {

    }

    @Override
    public void sendSuccess(long requestId) {

    }

    @Override
    public void sendError(long requestId, ServerError error, String message) {

    }

    @Override
    public void sendReachedEndOfTopic(long consumerId) {

    }

    @Override
    public Future<Void> sendMessagesToConsumer(long consumerId, String topicName, Subscription subscription,
            int partitionIdx, List<Entry> entries, EntryBatchSizes batchSizes, EntryBatchIndexesAcks batchIndexesAcks,
            RedeliveryTracker redeliveryTracker) {
        return ImmediateEventExecutor.INSTANCE.newSucceededFuture(null);
    }

    @Override
    public void sendTcClientConnectResponse(long requestId, ServerError error, String message) {

    }

    @Override
    public void sendTcClientConnectResponse(long requestId) {

    }
}
