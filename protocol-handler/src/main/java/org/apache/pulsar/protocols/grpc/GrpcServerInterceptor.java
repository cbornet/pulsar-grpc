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
package org.apache.pulsar.protocols.grpc;

import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import org.apache.pulsar.protocols.grpc.api.CommandProducer;
import org.apache.pulsar.protocols.grpc.api.CommandSubscribe;

import java.net.SocketAddress;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.pulsar.protocols.grpc.Constants.CONSUMER_PARAMS_CTX_KEY;
import static org.apache.pulsar.protocols.grpc.Constants.CONSUMER_PARAMS_METADATA_KEY;
import static org.apache.pulsar.protocols.grpc.Constants.PRODUCER_PARAMS_CTX_KEY;
import static org.apache.pulsar.protocols.grpc.Constants.PRODUCER_PARAMS_METADATA_KEY;
import static org.apache.pulsar.protocols.grpc.Constants.REMOTE_ADDRESS_CTX_KEY;

class GrpcServerInterceptor implements ServerInterceptor {

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> serverCall, Metadata metadata,
            ServerCallHandler<ReqT, RespT> serverCallHandler) {
        Context ctx = Context.current();

        if (metadata.containsKey(PRODUCER_PARAMS_METADATA_KEY)) {
            try {
                CommandProducer params = CommandProducer.parseFrom(metadata.get(PRODUCER_PARAMS_METADATA_KEY));
                checkArgument(!params.getTopic().isEmpty(), "Empty topic name");
                ctx = ctx.withValue(PRODUCER_PARAMS_CTX_KEY, params);
            } catch (InvalidProtocolBufferException | IllegalArgumentException e) {
                throw Status.INVALID_ARGUMENT.withDescription("Invalid producer metadata: " + e.getMessage())
                        .asRuntimeException(metadata);
            }
        }
        if (metadata.containsKey(CONSUMER_PARAMS_METADATA_KEY)) {
            try {
                CommandSubscribe params = CommandSubscribe.parseFrom(metadata.get(CONSUMER_PARAMS_METADATA_KEY));
                checkArgument(!params.getTopic().isEmpty(), "Empty topic name");
                ctx = ctx.withValue(CONSUMER_PARAMS_CTX_KEY, params);
            } catch (InvalidProtocolBufferException | IllegalArgumentException e) {
                throw Status.INVALID_ARGUMENT.withDescription("Invalid consumer metadata: " + e.getMessage())
                        .asRuntimeException(metadata);
            }
        }

        SocketAddress socketAddress = serverCall.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
        ctx = ctx.withValue(REMOTE_ADDRESS_CTX_KEY, socketAddress);

        return Contexts.interceptCall(ctx, serverCall, metadata, serverCallHandler);
    }

}
