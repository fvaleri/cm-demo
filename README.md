# Secure Kafka migration with Cluster Mirroring

> [!NOTE]  
> This repo uses an unoffical POC-level Kafka 4.2 revision for early access testing.
> It is not production ready, so do not run it against real workloads.

This Cluster Mirroring ([KIP-1279][1]) demo walks through a cluster migration from cluster A (source) to cluster B (destination).
Cluster A runs on a Kafka 3.9.2 binary with ZK mode, JKS keystores and legacy AclAuthorizer. 
Cluster B runs on an unofficial Kafka 4.2.0 binary with KRaft mode, PEM certificates and StandardAuthorizer.
Both clusters run on localhost and use SASL_SSL with PLAIN authentication sharing a self-signed TLS certificate.

Prerequisites: bash 4+, curl, git, tar, openssl, keytool (from Java JDK), jq, java 11+.

## Initial setup

Configure the environment and start source and destination clusters.
A simple web console will be available at http://localhost:8099.

```sh
git clone --depth 1 git@github.com:fvaleri/cm-demo.git
cd cm-demo
source init.sh
test-setup
```

## List topics

List topics on both clusters. Destination should be empty.

```sh
echo "---- source ----"
"$SRC_HOME"/bin/kafka-topics.sh --bootstrap-server "$SRC_BOOTSTRAP" --list --command-config "$TEST_DIR"/kafka.properties
echo "---- destination ----"
"$DST_HOME"/bin/kafka-topics.sh --bootstrap-server "$DST_BOOTSTRAP" --list --command-config "$TEST_DIR"/kafka.properties
```

## Start clients

Start 3 producers at 1000 msg/s each (512 bytes per record) and 2 consumers in my-group on source.
No key is set so the default partitioner round-robins across all partitions.

```sh
for topic in my-topic-a my-topic-b new-topic-a; do
  "$SRC_HOME"/bin/kafka-producer-perf-test.sh --topic "$topic" --throughput 1000 \
    --num-records 100000 --record-size 512 --producer.config "$TEST_DIR"/client.properties \
    --producer-props bootstrap.servers="$SRC_BOOTSTRAP" >/dev/null 2>&1 &
  disown
done
"$SRC_HOME"/bin/kafka-console-consumer.sh --bootstrap-server "$SRC_BOOTSTRAP" --include "my-topic-.*" \
  --group my-group --from-beginning --consumer.config "$TEST_DIR"/client.properties >/dev/null >/dev/null 2>&1 &
disown
"$SRC_HOME"/bin/kafka-console-consumer.sh --bootstrap-server "$SRC_BOOTSTRAP" --include "new-topic-a" \
  --group my-group --from-beginning --consumer.config "$TEST_DIR"/client.properties >/dev/null >/dev/null 2>&1 &
disown
```

## Create a mirror

Create mirror a-to-b and start mirroring topics matching my-topic.* regex.

```sh
"$DST_HOME"/bin/kafka-cluster-mirrors.sh --bootstrap-server "$DST_BOOTSTRAP" --create --mirror a-to-b \
  --mirror-config "$TEST_DIR"/a-to-b.properties --command-config "$TEST_DIR"/kafka.properties
"$DST_HOME"/bin/kafka-cluster-mirrors.sh --bootstrap-server "$DST_BOOTSTRAP" --start --topics "my-topic.*" \
  --mirror a-to-b --command-config "$TEST_DIR"/kafka.properties
```

## Create another mirror

Create mirror new-a-to-b and start mirroring new-topic-a.

```sh
"$DST_HOME"/bin/kafka-cluster-mirrors.sh --bootstrap-server "$DST_BOOTSTRAP" --create --mirror new-a-to-b \
  --mirror-config "$TEST_DIR"/a-to-b.properties --command-config "$TEST_DIR"/kafka.properties
"$DST_HOME"/bin/kafka-cluster-mirrors.sh --bootstrap-server "$DST_BOOTSTRAP" --start --topics new-topic-a \
  --mirror new-a-to-b --command-config "$TEST_DIR"/kafka.properties
```

## Wait and check state

Wait for all mirrors to reach MIRRORING state, then show their status.

```sh
wait-for-state "$DST_BOOTSTRAP" "MIRRORING" ".*" ".*" "$TEST_DIR"/kafka.properties
"$DST_HOME"/bin/kafka-cluster-mirrors.sh --bootstrap-server "$DST_BOOTSTRAP" --describe \
  --command-config "$TEST_DIR"/kafka.properties
```

## Stop clients

Stop all producers and consumers, then wait for replication lag to reach zero and metadata to sync.

```sh
pkill -SIGKILL -ef "ProducerPerformance" ||true
pkill -SIGKILL -ef "ConsoleConsumer" ||true
wait-for-lag-zero "$DST_BOOTSTRAP" ".*" ".*" "$TEST_DIR"/kafka.properties
wait-for-meta-sync "$DST_BOOTSTRAP" "$TEST_DIR"/kafka.properties
```

## Verify synchronization

Verify log convergence by comparing last 5 records on my-topic-b partition 2.

```sh
echo "---- source ----"
"$SRC_HOME"/bin/kafka-dump-log.sh --files "$TEST_DIR"/server1/data/my-topic-b-2/*.log --print-data-log 2>/dev/null | tail -5
echo "---- destination ----"
"$DST_HOME"/bin/kafka-dump-log.sh --files "$TEST_DIR"/server4/data/my-topic-b-2/*.log --print-data-log 2>/dev/null | tail -5
```

Verify topic configuration on new-topic-a.

```sh
echo "---- source ----"
"$SRC_HOME"/bin/kafka-configs.sh --bootstrap-server "$SRC_BOOTSTRAP" --entity-type topics --entity-name new-topic-a \
  --describe --command-config "$TEST_DIR"/kafka.properties
echo "---- destination ----"
"$DST_HOME"/bin/kafka-configs.sh --bootstrap-server "$DST_BOOTSTRAP" --entity-type topics --entity-name new-topic-a \
  --describe --command-config "$TEST_DIR"/kafka.properties
```

Verify access control list.

```sh
echo "---- source ----"
"$SRC_HOME"/bin/kafka-acls.sh --bootstrap-server "$SRC_BOOTSTRAP" --list --command-config "$TEST_DIR"/kafka.properties
echo "---- destination ----"
"$DST_HOME"/bin/kafka-acls.sh --bootstrap-server "$DST_BOOTSTRAP" --list --command-config "$TEST_DIR"/kafka.properties
```

Compare consumer group offsets for my-group.
Content should be indentical, but ordering may be different because the old Kafka version lacks topic+partition sorting.

```sh
echo "---- source ----"
"$SRC_HOME"/bin/kafka-consumer-groups.sh --bootstrap-server "$SRC_BOOTSTRAP" --group my-group \
  --describe --command-config "$TEST_DIR"/kafka.properties
echo -e "\n---- destination ----"
"$DST_HOME"/bin/kafka-consumer-groups.sh --bootstrap-server "$DST_BOOTSTRAP" --group my-group \
  --describe --command-config "$TEST_DIR"/kafka.properties
```

## Failover

Migration complete, stop all mirrors to perform failover.
This produces a pid-reset control record for each mirrored partition.

```sh
"$DST_HOME"/bin/kafka-cluster-mirrors.sh --bootstrap-server "$DST_BOOTSTRAP" --stop \
  --mirror a-to-b --topics ".*" --command-config "$TEST_DIR"/kafka.properties
"$DST_HOME"/bin/kafka-cluster-mirrors.sh --bootstrap-server "$DST_BOOTSTRAP" --stop \
  --mirror new-a-to-b --topics ".*" --command-config "$TEST_DIR"/kafka.properties
wait-for-state "$DST_BOOTSTRAP" "STOPPED" ".*" ".*" "$TEST_DIR"/kafka.properties
wait-for-meta-sync "$DST_BOOTSTRAP" "$TEST_DIR"/kafka.properties
"$DST_HOME"/bin/kafka-cluster-mirrors.sh --bootstrap-server "$DST_BOOTSTRAP" --describe \
  --command-config "$TEST_DIR"/kafka.properties
```

## Drain all messages

Drain all remaining messages from destination using my-group
The total consumed messages should be equal to the sum of lag values minus 10 pid-reset control records.
Final consumer group state should have zero lag.

```sh
"$DST_HOME"/bin/kafka-console-consumer.sh --bootstrap-server "$DST_BOOTSTRAP" \
  --group my-group --include ".*" --timeout-ms 10000 \
  --consumer-property group.protocol=consumer \
  --consumer-property auto.commit.interval.ms=10 \
  --consumer.config "$TEST_DIR"/client.properties 2>&1 >/dev/null | grep "Processed"
"$DST_HOME"/bin/kafka-consumer-groups.sh --bootstrap-server "$DST_BOOTSTRAP" --group my-group \
  --describe --command-config "$TEST_DIR"/kafka.properties
```

## Final cleanup

Stop all running processes.

```sh
test-teardown
```

[1]: https://cwiki.apache.org/confluence/spaces/KAFKA/pages/406620973/KIP-1279+Cluster+Mirroring
[2]: https://github.com/fvaleri/cm-demo/issues
