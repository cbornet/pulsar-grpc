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

import io.grpc.stub.StreamObserver;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.PulsarCommandSender;
import io.github.cbornet.pulsar.handlers.grpc.api.ConsumeOutput;
import io.github.cbornet.pulsar.handlers.grpc.api.PayloadType;

import java.net.SocketAddress;

class ConsumerCnx extends AbstractGrpcCnx {

    private final StreamObserver<ConsumeOutput> responseObserver;
    private final ConsumerCommandSender consumerCommandSender;

    public ConsumerCnx(BrokerService service, SocketAddress remoteAddress, String authRole,
            AuthenticationDataSource authenticationData, StreamObserver<ConsumeOutput> responseObserver,
            PayloadType preferedPayloadType, java.util.function.Consumer<Integer> cb) {
        super(service, remoteAddress, authRole, authenticationData);
        this.responseObserver = responseObserver;
        this.consumerCommandSender = new ConsumerCommandSender(responseObserver, preferedPayloadType, cb);
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
    public PulsarCommandSender getCommandSender() {
        return consumerCommandSender;
    }

    @Override
    public String getAuthRole() {
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
}
