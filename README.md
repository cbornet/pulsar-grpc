# gRPC protocol handler for Pulsar

The gRPC protocol handler for Pulsar provides a gRPC interface as an alternative to the Pulsar binary protocol.

The goal of this handler is to provide an API easier to integrate than the binary TCP protocol when there is no 
existing Pulsar driver for a given language but there exists a gRPC implementation.

## Enable the gRPC protocol handler on your existing Apache Pulsar clusters

### Build the gRPC protocol handler

1. clone this project from GitHub to your local.
```bash
git clone https://github.com/streamnative/pulsar-protocol-handler-grpc.git
cd pulsar-protocol-handler-grpc
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

You need to configure the Pulsar broker to run the gRPC protocol handler as a plugin, that is,
add configurations in Pulsar's configuration file, such as `broker.conf` or `standalone.conf`.

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
    The gRPC protocol handler uses the same configuration properties as the Pulsar broker 
    (see [Transport Encryption using TLS](https://pulsar.apache.org/docs/en/security-tls-transport/) for details).

    **Example**

    ```
    grpcServicePort=9080
    grpcServiceTlsPort=9443
    ```

### Restart Pulsar brokers to load the gRPC protocol handler

After you have installed the gRPC protocol handler to Pulsar broker, you can restart the Pulsar brokers to load it.
