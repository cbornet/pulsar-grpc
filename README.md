# gRPC protocol handler for Pulsar

The gRPC protocol handler for Pulsar provides a gRPC interface as an alternative to the Pulsar binary protocol.

The goal of this handler is to provide an API easier to integrate than the binary TCP protocol when there is no existing Pulsar driver for a given language or when a wanted feature is not yet implemented in a Pulsar driver but there exists a gRPC implementation.

## Enable the gRPC protocol handler on your existing Apache Pulsar clusters

### Build the gRPC protocol handler

1. clone this project from GitHub to your local.
```bash
git clone https://github.com/cbornet/pulsar-grpc.git
cd pulsar-grpc
```

2. build the project.
```bash
mvn clean package -DskipTests
```

3. the nar file can be found at this location.
```bash
./protocol-handler/target/pulsar-protocol-handler-grpc-${version}.nar
```

### Install the gRPC protocol handler

As mentioned previously, the gPRC protocol handler is a plugin that can be installed to the Pulsar brokers.

Version 2.7+ of Apache Pulsar brokers is required for this plugin to work.

You need to configure the Pulsar broker to run the gRPC protocol handler as a plugin, that is, add configurations in Pulsar's configuration file, such as `broker.conf` or `standalone.conf`.

1. Set the configuration of the gRPC protocol handler.

    Add the following properties and set their values in Pulsar configuration file, such as `conf/broker.conf` or `conf/standalone.conf`.
    
    Property | Set it to the following value | Default value
    |---|---|---
    `messagingProtocols` | grpc | null
    `protocolHandlerDirectory`| Location of the NAR file | ./protocols
    
    **Example**

    ```
    messagingProtocols=grpc
    protocolHandlerDirectory=./protocols
    ```

2. Set the gRPC service ports.

    Set the `grpcServicePort` property to start a plaintext gRPC server.
    
    Set the `grpcServiceTlsPort` property to start a TLS secured gRPC server.
    The gRPC protocol handler uses the same configuration properties as the Pulsar broker (see [Transport Encryption using TLS](https://pulsar.apache.org/docs/en/security-tls-transport/) for details).

    **Example**

    ```
    grpcServicePort=9080
    grpcServiceTlsPort=9443
    ```

### Restart Pulsar brokers to load the gRPC protocol handler

After you have installed the gRPC protocol handler to Pulsar broker, you can restart the Pulsar brokers to load it.

## Use the gRPC API

The API is specified in the file [PulsarApi.proto]( protocol-handler/src/main/proto/PulsarApi.proto) and can be used to generate Pulsar gRPC clients in the chosen language.
The gRPC API has a lot in common with the [Pulsar binary protocol](https://pulsar.apache.org/docs/en/develop-binary-protocol/): the protobufs exchanged are almost the same. The following documentation takes the binary protocol definitions of the protos as reference and only highlights the differences.

One difference with the binary protocol is that the message metadata and payload are embedded inside the `CommandSend`/`CommandMessage` proto (the performance optimization of the binary protocol cannot be done with standard gRPC).
In `CommandSend`/`CommandMessage`, there are 3 possible modes to encode the message and payload:

1. `bytes` : this format is the closest from the binary protocol. The client will have to encode/decode the metadata and payload with the binary framing. It's the only mode possible if the message is encrypted.
2. `MetadataAndPayload` : in this format the metadata and payload are sent/received in distinct protobuf fields instead of the Pulsar framing. In a `CommanSend`, if the `compress` field is set, the message will be compressed by the broker using the compressions settings of the metadata. It's also possible to compress on the client.
3. `Messages` : this format allows to send/receive multiple messages in batch using protobuf format instead of the binary framing. The compression settings contained in the `metadata` field are used by the broker to compress/uncompress the message.

Other difference with the binary protocol is that gRPC has its own keep-alive and reconnection management so you don't have to implement it on the client.

For producers/consumers, the gRPC flow control is used so you don't have to handle rate limit sending errors or consumer flow messages.


### Topic Lookup

```protobuf
rpc lookup_topic(CommandLookupTopic) returns (CommandLookupTopicResponse) {}
```
[Example in Java](https://github.com/cbornet/pulsar-grpc/blob/ade0b5af65c3f139ae23e7dc694b1871ad6a7072/protocol-handler/src/test/java/org/apache/pulsar/protocols/grpc/GrpcServiceTest.java#L186)

Topic lookup works similarly to the [binary protocol](https://pulsar.apache.org/docs/en/develop-binary-protocol/#topic-lookup) except that it returns the `grpcServiceHost`, `grpcServicePort` and `grpcServicePortTls` owning the given topic in the response.

> It's also possible to lookup the broker by REST or binary protocol and then making a call to the topic's broker `/admin/v2/broker-stats/load-report` endpoint to get the info in the `protocols.grpc` field in the form `grpcServiceHost=xxx;grpcServicePort=xxx;grpcServicePortTls=xxx`.


### Producing messages

#### Producing a single message

```protobuf
rpc produceSingle(CommandProduceSingle) returns (CommandSendReceipt) {}
```
[Example in Java](https://github.com/cbornet/pulsar-grpc/blob/ade0b5af65c3f139ae23e7dc694b1871ad6a7072/protocol-handler/src/test/java/org/apache/pulsar/protocols/grpc/PulsarGrpcServiceTest.java#L1428)

This is a simplified interface to send one messages one at a time. Note that authentication/authorization will occur at each call so prefer the streaming interface if you have a lot of messages to send.
`CommandProduceSingle` assembles a `CommandProducer` used to create a producer and a `CommandSend` containing the message to send.
The producer is automatically closed at the end of the rpc call so there's no `CloseProducer` command needed.


#### Producing a stream of messages

```protobuf
rpc produce(stream CommandSend) returns (stream SendResult) {}
```
[Example in Java](https://github.com/cbornet/pulsar-grpc/blob/ade0b5af65c3f139ae23e7dc694b1871ad6a7072/protocol-handler/src/test/java/org/apache/pulsar/protocols/grpc/PulsarGrpcServiceTest.java#L448)

This call creates a producer to send messages continuously and receive acknowledgments asynchronously.
The `CommandProducer` used to create the producer must be passed as [gRPC call metadata](https://grpc.io/docs/what-is-grpc/core-concepts/#metadata) with the key `pulsar-producer-params-bin` and encoded in protobuf.

`SendResult` can be one of `CommandProducerSuccess`, `CommandSendReceipt`, `CommandSendError`.

If producer/broker/topic rate limit is reached, the gRPC flow control will be triggered. So you don't have to worry about rate limiting sending errors.

The producer is automatically closed at the end of the rpc call so there's no `CloseProducer` command needed.


### Consuming messages

```protobuf
rpc consume(stream ConsumeInput) returns (stream ConsumeOutput) {}
```
[Example in Java](https://github.com/cbornet/pulsar-grpc/blob/ade0b5af65c3f139ae23e7dc694b1871ad6a7072/protocol-handler/src/test/java/org/apache/pulsar/protocols/grpc/ProducerConsumerCompatibilityTest.java#L246)

This call creates a consumer to receive messages continuously and send acknowledgments.

The `CommandSubscribe` used to create the producer must be passed as [gRPC call metadata](https://grpc.io/docs/what-is-grpc/core-concepts/#metadata) with the key `pulsar-consumer-params-bin` and encoded in protobuf.

Compared to the binary protocol, `CommandSubscribe` has an additional `preferedPayloadType` field to indicate how the `CommandMessage` should be sent preferably. It can take the values:
* `MESSAGES` (default): batch messages exposed in protobuf
* `BINARY`: raw metadata and payload encoded with the binary framing 
* `METADATA_AND_PAYLOAD`: metadata and payload in separate protobuf fields
* `METADATA_AND_PAYLOAD_UNCOMPRESSED`: metadata and payload in separate protobuf fields with payload uncompressed on the broker.

If the message is encrypted, the BINARY mode will be used as the broker cannot decrypt it.

`ConsumeInput` can be one of `CommandAck`, `CommandFlow`, `CommandUnsubscribe`, `CommandRedeliverUnacknowledgedMessages`,`CommandConsumerStats`,`CommandGetLastMessageId`,`CommandSeek`.

`ConsumeOutput` can be one of `CommandSubscribeSuccess`, `CommandMessage`, `CommandAckResponse`, `CommandActiveConsumerChange`, `CommandReachedEndOfTopic`, `CommandConsumerStatsResponse`, `CommandGetLastMessageIdResponse`, `CommandSuccess`, `CommandError`.

The gRPC flow control is used to automatically backpressure the arrival of new messages. So there's no need to send `CommandFlow` messages to ask for new messages. `CommandFlow` shall be called once to buffer some messages on the broker for throughput tuning.

The consumer is automatically closed at the end of the rpc call so there's no `CloseConsumer` command needed.


### Authenticating

All [Pulsar modes of authentication](https://pulsar.apache.org/docs/en/security-overview/) are supported (TLS, Basic, Athenz, JWT, Kerberos).

Authentication is done by attaching a `CommandAuth` as a binary metadata header with key `pulsar-auth-bin` that contain the same `auth_method` and `auth_data` fields as the Pulsar binary `CommandConnect` message.

[Example in Java](https://github.com/cbornet/pulsar-grpc/blob/30b7cc119bf282e6d9ee4ba6e73cbc03cc2315a1/protocol-handler/src/test/java/org/apache/pulsar/protocols/grpc/AuthenticationInterceptorTest.java#L202)

For Kerberos/SASL the flow is similar to the one for the REST interface:
* The client sends a `CommandAuth` message in a call, the broker will immediately close the call with a `CommandAuthChallenge` as a binary metadata header with key `pulsar-authchallenge-bin`.
* The client must verify and solve the challenge then send the result in a call with `CommandAuthResponse` as a binary metadata header with key `pulsar-authresponse-bin`.
* The broker will verify the challenge response and if successful will close the call with an `AuthRoleToken` as a binary metadata header with key `pulsar-authroletoken-bin`.
* The `AuthRoleToken` shall then be used to authentify subsequent calls by passing it as a binary metadata header with key `pulsar-authroletoken-bin`.

[Example in Java](https://github.com/cbornet/pulsar-grpc/blob/30b7cc119bf282e6d9ee4ba6e73cbc03cc2315a1/protocol-handler/src/test/java/org/apache/pulsar/protocols/grpc/AuthenticationInterceptorTest.java#L343)

### Transactions

The gRPC protocol handler has full support for transactions.
PIP 31 describes [the flow](https://docs.google.com/document/d/145VYp09JKTw9jAT-7yNyFU255FptB2_B2Fye100ZXDI/edit#heading=h.ej7chjkq8eph) and the [message definition](https://docs.google.com/document/d/145VYp09JKTw9jAT-7yNyFU255FptB2_B2Fye100ZXDI/edit#heading=h.onmmwpybu4v2) for transactions.
In gRPC this results in the following RPCs
```protobuf
rpc create_transaction(CommandNewTxn) returns (CommandNewTxnResponse) {}
rpc add_partitions_to_transaction(CommandAddPartitionToTxn) returns (CommandAddPartitionToTxnResponse) {}
rpc end_transaction(CommandEndTxn) returns (CommandEndTxnResponse) {}
rpc end_transaction_on_partition(CommandEndTxnOnPartition) returns (CommandEndTxnOnPartitionResponse) {}
rpc end_transaction_on_subscription(CommandEndTxnOnSubscription) returns (CommandEndTxnOnSubscriptionResponse) {}
```
[Example in Java](https://github.com/cbornet/pulsar-grpc/blob/33b8d92f276ea346e75e05e665d329a960e57c54/protocol-handler/src/test/java/org/apache/pulsar/protocols/grpc/PulsarGrpcServiceTest.java#L1385)

