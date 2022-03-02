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
package io.github.cbornet.pulsar.handlers.grpc.benchmark;

import com.google.common.collect.Sets;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * Benchmarking utility program.
 * Creates one producer and one consumer on a random topic and tries to max out the throughput of messages.
 */
public class BenchmarkBinary {

    public static final String PULSAR_SERVICE_URL = "http://localhost:8080";
    public static final String TENANT = "binary";
    public static final String CLUSTER = "standalone";
    public static final String NAMESPACE_PREFIX = "binary/benchmark";
    public static final int BYTES_IN_ONE_MEGABYTE = 1024 * 1024;

    public static final int DEFAULT_MESSAGE_SIZE = 1024;
    public static final int PENDING_MESSAGE_BUFFER_SIZE = 10_000;
    public static final int ENSEMBLE_SIZE = 1;
    public static final int WRITE_QUORUM = 1;
    public static final int ACK_QUORUM = 1;
    public static final boolean DEDUPLICATION_ENABLED = false;

    public static void main(String[] args) throws PulsarClientException, PulsarAdminException {

        int messageSize = DEFAULT_MESSAGE_SIZE;
        if (args.length > 0) {
            messageSize = Integer.parseInt(args[0]);
        }

        System.out.println("Starting benchmark with message size: " + messageSize);
        long sends = 0;
        LongAdder sendAcks = new LongAdder();
        LongAdder receivedMsg = new LongAdder();

        PulsarAdminBuilder pulsarAdminBuilder = PulsarAdmin.builder().serviceHttpUrl(PULSAR_SERVICE_URL);

        PulsarAdmin adminClient = pulsarAdminBuilder.build();

        // Create namespace and set the configuration
        if (!adminClient.tenants().getTenants().contains(TENANT)) {
            try {
                adminClient.tenants().createTenant(TENANT,
                    TenantInfo.builder()
                        .allowedClusters(Sets.newHashSet(CLUSTER))
                        .build());
            } catch (PulsarAdminException.ConflictException e) {
                // Ignore. This can happen when multiple workers are initializing at the same time
            }
        }

        String namespace = NAMESPACE_PREFIX + "-" + UUID.randomUUID();
        adminClient.namespaces().createNamespace(namespace);
        System.out.println("Created Pulsar namespace " + namespace);

        adminClient.namespaces().setPersistence(namespace,
                new PersistencePolicies(ENSEMBLE_SIZE, WRITE_QUORUM, ACK_QUORUM, 1.0));

        adminClient.namespaces().setBacklogQuota(namespace,
            BacklogQuota.builder()
                .limitSize(-1L)
                .limitTime(-1)
                .retentionPolicy(BacklogQuota.RetentionPolicy.producer_exception)
                .build());
        adminClient.namespaces().setDeduplicationStatus(namespace, DEDUPLICATION_ENABLED);

        String topic = "persistent://" + namespace + "/test-" + UUID.randomUUID();
        System.out.println("Topic: " + topic);

        ClientBuilder clientBuilder = PulsarClient.builder()
                .ioThreads(8)
                .connectionsPerBroker(8)
                .statsInterval(0, TimeUnit.SECONDS)
                .serviceUrl(PULSAR_SERVICE_URL)
                .maxConcurrentLookupRequests(50000)
                .maxLookupRequests(100000)
                .listenerThreads(Runtime.getRuntime().availableProcessors());

        PulsarClient client = clientBuilder.build();

        Producer<byte[]> producer = client.newProducer().enableBatching(false)
                .blockIfQueueFull(true)
                .maxPendingMessages(PENDING_MESSAGE_BUFFER_SIZE)
                .topic(topic).create();

        client.newConsumer().subscriptionType(SubscriptionType.Failover)
                .messageListener((consumer, msg) -> {
                    receivedMsg.increment();
                    consumer.acknowledgeAsync(msg);
                })
                .topic(topic)
                .subscriptionName("sub-" + UUID.randomUUID())
                .subscribe();

        long previousAcks = 0;
        long previousReceivedMsg = 0;
        long previousTime = System.currentTimeMillis();

        byte[] dataBytes = new byte[messageSize];
        Random random = new Random();
        random.nextBytes(dataBytes);

        while (true) {
            long acks = sendAcks.sum();
            long totalMsg = receivedMsg.sum();
            if (sends - acks < PENDING_MESSAGE_BUFFER_SIZE) {
                producer.sendAsync(dataBytes)
                        .thenAccept(messageId -> sendAcks.increment());
                sends++;
            }
            long now = System.currentTimeMillis();
            long timedelta = now - previousTime;
            if (timedelta > 10_000) {
                System.out.println("Producer speed: "
                        + (acks - previousAcks) * 1000f / timedelta + " msg/s - "
                        + (acks - previousAcks) * 1000f * messageSize / (timedelta * BYTES_IN_ONE_MEGABYTE) + " MBps");
                System.out.println("Consumer speed: "
                        + (totalMsg - previousReceivedMsg) * 1000f / timedelta + " msg/s - "
                        + (totalMsg - previousReceivedMsg) * 1000f * messageSize / (timedelta * BYTES_IN_ONE_MEGABYTE)
                        + " MBps");
                previousTime = now;
                previousAcks = acks;
                previousReceivedMsg = totalMsg;
            }
        }

    }
}
