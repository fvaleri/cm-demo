#
# KAFKA CLUSTER MIGRATION DEMO WITH CLUSTER MIRRORING (KIP-1279)
#
# The following demo walks through a cluster migration from cluster A (Kafka 3.9.2,
# ZooKeeper mode, JKS keystores for SSL, legacy AclAuthorizer) to clustr B (latest
# Kafka build running in KRaft mode, PEM certificates via DirectoryConfigProvider,
# StandardAuthorizer). Both clusters use SASL_SSL with PLAIN authentication and
# share a self-signed TLS certificate. There is also a simple local dashboard
# for monitoring the mirroring state.
#

TEST_DIR="$(pwd)/results/fvaleri"; mkdir -p "$TEST_DIR"
declare -A server0=([id]="0" [rep]="2181" [jmx]="7000" [fol]="5000" [oth]="6000")
declare -A server1=([id]="1" [rep]="6001" [jmx]="7001" [cli]="9091")
declare -A server2=([id]="2" [rep]="6002" [jmx]="7002" [cli]="9092")
declare -A server3=([id]="3" [rep]="6003" [jmx]="7003")
declare -A server4=([id]="4" [rep]="6004" [jmx]="7004" [cli]="9094")
declare -A server5=([id]="5" [rep]="6005" [jmx]="7005" [cli]="9095")
SRC_BOOTSTRAP="localhost:${server1[cli]},localhost:${server2[cli]}"
SRC_VERSION="3.9.2"
SRC_HOME="/tmp/kafka-$SRC_VERSION"
DST_BOOTSTRAP="localhost:${server4[cli]},localhost:${server5[cli]}"
DASH_HOME="$HOME/Documents/cm-demo/dash"

build-con-boot() {
  local start="$1" end="$2"
  local boot=""
  for i in $(seq "$start" "$end"); do
    declare -n ref="server$i"
    boot+="${boot:+,}localhost:${ref[rep]}"
  done
  echo "$boot"
}

start-node() {
  declare -n ref="server$1"
  echo "Starting node ${ref[id]}"
  KAFKA_HEAP_OPTS="-Xmx1G -Xms1G" JMX_PORT="${ref[jmx]}" LOG_DIR="$TEST_DIR/server${ref[id]}/logs" \
    KAFKA_LOG4J_OPTS="-Dlog4j2.configurationFile=file:$TEST_DIR/server${ref[id]}/config/log4j2.yaml" \
    bin/kafka-server-start.sh -daemon "$TEST_DIR"/server"${ref[id]}"/config/server.properties
}

stop-node() {
  declare -n ref="server$1"
  echo "Stopping node ${ref[id]}"
  local pid=$(pgrep -f "server${ref[id]}/config" || true)
  if [[ -n "$pid" ]]; then
    kill -SIGTERM $pid &>/dev/null
    for ((i=0; i<30; i++)); do kill -0 $pid 2>/dev/null || break; sleep 1; done
    kill -0 $pid 2>/dev/null && { kill -SIGKILL $pid &>/dev/null; sleep 1; }
  fi
  return 0
}

restart-node() {
  declare -n ref="server$1"
  stop-node "${ref[id]}"
  start-node "${ref[id]}"
  sleep 5
}

wait-for-meta-sync() {
  local bootstrap="$1" cmd_config="${2-}"
  local cmd_opts=""
  [[ -n "$cmd_config" ]] && cmd_opts="--command-config $cmd_config"
  local interval_ms
  interval_ms=$(bin/kafka-configs.sh --bootstrap-server "$bootstrap" --entity-type brokers --all --describe $cmd_opts 2>/dev/null \
    | grep -oP 'mirror.metadata.refresh.interval.ms=\K[0-9]+' | head -1 || echo 30000)
  local interval_s=$(( interval_ms / 1000 ))
  echo "Waiting ${interval_s}s for metadata sync"
  sleep "$interval_s"
}

wait-for-state() {
  local bootstrap="$1" state="$2" mirror_re="${3-.*}" topic_re="${4-.*}" cmd_config="${5-}"
  local cmd_opts=""
  [[ -n "$cmd_config" ]] && cmd_opts="--command-config $cmd_config"
  echo "Waiting for state $state (mirror: $mirror_re, topic: $topic_re)"
  for i in $(seq 1 30); do
    bin/kafka-cluster-mirrors.sh --bootstrap-server "$bootstrap" --describe --json $cmd_opts 2>/dev/null | \
      jq -e --arg s "$state" --arg m "$mirror_re" --arg t "$topic_re" \
        '[.[] | select((.mirror | test($m)) and (.topic | test($t)))] | length > 0 and all(.state == $s)' &>/dev/null && return 0
    sleep 2
  done
  return 1
}

wait-for-lag-zero() {
  local bootstrap="$1" mirror_re="${2-.*}" topic_re="${3-.*}" cmd_config="${4-}"
  local cmd_opts=""
  [[ -n "$cmd_config" ]] && cmd_opts="--command-config $cmd_config"
  echo "Waiting for lag zero (mirror: $mirror_re, topic: $topic_re)"
  for i in $(seq 1 30); do
    bin/kafka-cluster-mirrors.sh --bootstrap-server "$bootstrap" --describe --json $cmd_opts 2>/dev/null | \
      jq -e --arg m "$mirror_re" --arg t "$topic_re" \
        '[.[] | select((.mirror | test($m)) and (.topic | test($t)))] | length > 0 and all(.state == "MIRRORING" and .lag == 0)' &>/dev/null && return 0
    sleep 2
  done
  return 1
}

# This needs to be exported to be used by the env config provider
export PASSWD="changeit"

create-crt() {
  local dir="$1"
  local cn="localhost" o="Fede" st="Rome" c="IT" domain="f12i.io"
  local config="
[req]
prompt=no
distinguished_name=dn
x509_extensions=ext
[dn]
countryName=$c
stateOrProvinceName=$st
organizationName=$o
commonName=$cn
[ext]
subjectAltName=@san
[san]
DNS.1=$cn
DNS.2=$cn.$domain
DNS.3=www.$cn.$domain
"
  echo "Creating self-signed TLS certificate"
  mkdir -p "$dir"
  openssl genrsa -out "$dir"/"$cn".key 2048
  openssl req -new -x509 -days 3650 -key "$dir"/"$cn".key -out "$dir"/"$cn".crt -config <(echo "$config")
}

create-jks() {
  local dir="$1"
  echo "Creating JKS keystore and truststore from PEM certificate"
  openssl pkcs12 -export -in "$dir"/localhost.crt -inkey "$dir"/localhost.key \
    -out "$dir"/keystore.p12 -name localhost -password pass:"$PASSWD"
  keytool -importkeystore -srckeystore "$dir"/keystore.p12 -srcstoretype PKCS12 \
    -srcstorepass "$PASSWD" -destkeystore "$dir"/keystore.jks -deststoretype JKS \
    -deststorepass "$PASSWD" -noprompt
  keytool -import -file "$dir"/localhost.crt -alias localhost \
    -keystore "$dir"/truststore.jks -storepass "$PASSWD" -noprompt
}

download-kafka() {
  local version="$1" scala="${2-2.13}"
  if [[ "$version" == 4.* ]]; then
    echo "Error: source cluster version 4.x is not supported" >&2
    return 1
  fi
  local dir="/tmp/kafka-$version"
  if [[ ! -d "$dir" ]]; then
    mkdir -p "$dir"
    curl -L "https://archive.apache.org/dist/kafka/$version/kafka_$scala-$version.tgz" \
      | tar -xz -C "$dir" --strip-components=1
  fi
}

# Source cluster ZK server (uses downloaded binary)
start-zserver() {
  local id="$1" SRC_HOME="$2"
  declare -n ref="server$id"
  mkdir -p "$TEST_DIR"/server"${ref[id]}"/config "$TEST_DIR"/server"${ref[id]}"/logs "$TEST_DIR"/server"${ref[id]}"/data
  cp "$SRC_HOME"/config/log4j.properties "$TEST_DIR"/server"${ref[id]}"/config/log4j.properties

  echo "${ref[id]}" >"$TEST_DIR"/server"${ref[id]}"/data/myid
  echo "dataDir=$TEST_DIR/server${ref[id]}/data" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "clientPort=${ref[rep]}" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "maxClientCnxns=0" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "admin.enableServer=false" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "initLimit=10" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "syncLimit=5" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "server.${server0[id]}=localhost:${server0[fol]}:${server0[oth]}" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties

  KAFKA_HEAP_OPTS="-Xmx1G -Xms1G" JMX_PORT="${ref[jmx]}" LOG_DIR="$TEST_DIR/server${ref[id]}/logs" \
    KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:$TEST_DIR/server${ref[id]}/config/log4j.properties" \
    "$SRC_HOME"/bin/zookeeper-server-start.sh -daemon "$TEST_DIR"/server"${ref[id]}"/config/server.properties
}

# Source cluster ZK broker (uses downloaded binary, JKS for SSL compatibility with older versions)
start-broker-zk() {
  local id="$1" zconn="$2" SRC_HOME="$3"
  declare -n ref="server$id"
  mkdir -p "$TEST_DIR/server${ref[id]}/config" "$TEST_DIR/server${ref[id]}/logs" "$TEST_DIR/server${ref[id]}/data"
  cp "$SRC_HOME"/config/log4j.properties "$TEST_DIR/server${ref[id]}/config"

  echo "broker.id=${ref[id]}" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "log.dirs=$TEST_DIR/server${ref[id]}/data" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "zookeeper.connect=$zconn" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "zookeeper.connection.timeout.ms=18000" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties

  # Listeners
  echo "listeners=REPLICATION://localhost:${ref[rep]},BROKER://localhost:${ref[cli]}" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "advertised.listeners=REPLICATION://localhost:${ref[rep]},BROKER://localhost:${ref[cli]}" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "listener.security.protocol.map=REPLICATION:SASL_SSL,BROKER:SASL_SSL" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "inter.broker.listener.name=REPLICATION" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties

  # Defaults used for auto created topics
  echo "default.replication.factor=2" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "min.insync.replicas=1" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "offsets.topic.replication.factor=2" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "transaction.state.log.replication.factor=2" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "transaction.state.log.min.isr=1" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties

  # Security
  echo 'super.users=User:kafka' >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "authorizer.class.name=kafka.security.authorizer.AclAuthorizer" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties

  echo "sasl.enabled.mechanisms=PLAIN" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "sasl.mechanism.inter.broker.protocol=PLAIN" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo 'listener.name.broker.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
    username="kafka" password="changeit" user_kafka="changeit" user_client="changeit";' >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo 'listener.name.replication.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
    username="kafka" password="changeit" user_kafka="changeit" user_client="changeit";' >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties

  echo "ssl.keystore.type=JKS" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "ssl.keystore.location=$TEST_DIR/keystore.jks" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "ssl.keystore.password=$PASSWD" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "ssl.key.password=$PASSWD" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "ssl.truststore.type=JKS" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "ssl.truststore.location=$TEST_DIR/truststore.jks" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "ssl.truststore.password=$PASSWD" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "ssl.endpoint.identification.algorithm=HTTPS" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties

  KAFKA_HEAP_OPTS="-Xmx1G -Xms1G" JMX_PORT="${ref[jmx]}" LOG_DIR="$TEST_DIR/server${ref[id]}/logs" \
    KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:$TEST_DIR/server${ref[id]}/config/log4j.properties" \
    "$SRC_HOME"/bin/kafka-server-start.sh -daemon "$TEST_DIR"/server"${ref[id]}"/config/server.properties
}

# Destination cluster controller (uses local build, PEM for SSL)
start-controller() {
  local id="$1" cid="$2" boot="$3"
  declare -n ref="server$id"
  mkdir -p "$TEST_DIR"/server"${ref[id]}"/config "$TEST_DIR"/server"${ref[id]}"/logs "$TEST_DIR"/server"${ref[id]}"/data
  cp config/log4j2.yaml "$TEST_DIR"/server"${ref[id]}"/config/log4j2.yaml

  echo "process.roles=controller" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "node.id=${ref[id]}" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "log.dirs=$TEST_DIR/server"${ref[id]}"/data" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "controller.quorum.bootstrap.servers=$boot" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties

  # Listeners
  echo "listeners=CONTROLLER://localhost:${ref[rep]}" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "advertised.listeners=CONTROLLER://localhost:${ref[rep]}" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "listener.security.protocol.map=CONTROLLER:SASL_SSL" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "controller.listener.names=CONTROLLER" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties

  # Defaults used for auto created topics
  echo "num.partitions=5" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "default.replication.factor=2" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "min.insync.replicas=1" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "offsets.topic.replication.factor=2" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "transaction.state.log.replication.factor=2" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "transaction.state.log.min.isr=1" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "share.coordinator.state.topic.replication.factor=2" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "share.coordinator.state.topic.min.isr=1" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "mirror.state.topic.replication.factor=2" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties

  # Security
  echo "config.providers=env,dir" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "config.providers.env.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "config.providers.env.param.allowlist.pattern=.*" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "config.providers.dir.class=org.apache.kafka.common.config.provider.DirectoryConfigProvider" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "config.providers.dir.param.allowlist.pattern=.*" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties

  echo 'super.users=User:kafka' >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties

  echo "sasl.enabled.mechanisms=" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "sasl.mechanism.controller.protocol=PLAIN" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "listener.name.controller.sasl.enabled.mechanisms=PLAIN" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo 'listener.name.controller.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
    username="kafka" password="${env:PASSWD}" user_kafka="${env:PASSWD}" user_client="${env:PASSWD}";' \
      >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties

  echo "listener.name.controller.ssl.keystore.type=PEM" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "listener.name.controller.ssl.keystore.certificate.chain=\${dir:$TEST_DIR:localhost.crt}" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "listener.name.controller.ssl.keystore.key=\${dir:$TEST_DIR:localhost.key}" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "listener.name.controller.ssl.truststore.type=PEM" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "listener.name.controller.ssl.truststore.certificates=\${dir:$TEST_DIR:localhost.crt}" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "ssl.endpoint.identification.algorithm=HTTPS" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties

  # Enable cluster mirroring in early access stage
  echo "unstable.api.versions.enable=true" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "unstable.feature.versions.enable=true" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties

  bin/kafka-storage.sh format -t "$cid" -c "$TEST_DIR"/server"${ref[id]}"/config/server.properties -s
  KAFKA_HEAP_OPTS="-Xmx1G -Xms1G" JMX_PORT="${ref[jmx]}" LOG_DIR="$TEST_DIR/server"${ref[id]}"/logs" \
    KAFKA_LOG4J_OPTS="-Dlog4j2.configurationFile=file:$TEST_DIR/server"${ref[id]}"/config/log4j2.yaml" \
    bin/kafka-server-start.sh -daemon "$TEST_DIR"/server"${ref[id]}"/config/server.properties
}

# Destination cluster broker (uses local build, PEM for SSL)
start-broker() {
  local id="$1" cid="$2" boot="$3"
  declare -n ref="server$id"
  mkdir -p "$TEST_DIR"/server"${ref[id]}"/config "$TEST_DIR"/server"${ref[id]}"/logs "$TEST_DIR"/server"${ref[id]}"/data
  cp config/log4j2.yaml "$TEST_DIR"/server"${ref[id]}"/config/log4j2.yaml

  echo "process.roles=broker" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "node.id=${ref[id]}" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "log.dirs=$TEST_DIR/server${ref[id]}/data" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "controller.quorum.bootstrap.servers=$boot" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "auto.create.topics.enable=false" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "mirror.num.replica.fetchers=2" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "mirror.metadata.refresh.interval.ms=5000" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties

  # Listeners
  echo "listeners=REPLICATION://localhost:${ref[rep]},BROKER://localhost:${ref[cli]}" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "advertised.listeners=REPLICATION://localhost:${ref[rep]},BROKER://localhost:${ref[cli]}" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "listener.security.protocol.map=CONTROLLER:SASL_SSL,REPLICATION:SASL_SSL,BROKER:SASL_SSL" \
    >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "controller.listener.names=CONTROLLER" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "inter.broker.listener.name=REPLICATION" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties

  # Defaults used for auto created topics for Kafka versions before 4.3
  echo "num.partitions=5" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "default.replication.factor=2" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "min.insync.replicas=1" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "offsets.topic.replication.factor=2" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "transaction.state.log.replication.factor=2" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "transaction.state.log.min.isr=1" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "share.coordinator.state.topic.replication.factor=2" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "share.coordinator.state.topic.min.isr=1" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "mirror.state.topic.replication.factor=2" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties

  # Security
  echo "config.providers=env,dir" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "config.providers.env.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "config.providers.dir.class=org.apache.kafka.common.config.provider.DirectoryConfigProvider" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties

  echo 'super.users=User:kafka' >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties

  echo "sasl.enabled.mechanisms=" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "sasl.mechanism.controller.protocol=PLAIN" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "listener.name.controller.sasl.enabled.mechanisms=PLAIN" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo 'listener.name.controller.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
    username="kafka" password="${env:PASSWD}" user_kafka="${env:PASSWD}" user_client="${env:PASSWD}";' \
      >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties

  echo "sasl.mechanism.inter.broker.protocol=PLAIN" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "listener.name.broker.sasl.enabled.mechanisms=PLAIN" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo 'listener.name.broker.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
    username="kafka" password="${env:PASSWD}" user_kafka="${env:PASSWD}" user_client="${env:PASSWD}";' \
      >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties

  echo "listener.name.replication.sasl.enabled.mechanisms=PLAIN" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo 'listener.name.replication.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
    username="kafka" password="${env:PASSWD}" user_kafka="${env:PASSWD}" user_client="${env:PASSWD}";' \
      >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties

  echo "listener.name.controller.ssl.keystore.type=PEM" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "listener.name.controller.ssl.keystore.certificate.chain=\${dir:$TEST_DIR:localhost.crt}" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "listener.name.controller.ssl.keystore.key=\${dir:$TEST_DIR:localhost.key}" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "listener.name.controller.ssl.truststore.type=PEM" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "listener.name.controller.ssl.truststore.certificates=\${dir:$TEST_DIR:localhost.crt}" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties

  echo "listener.name.broker.ssl.keystore.type=PEM" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "listener.name.broker.ssl.keystore.certificate.chain=\${dir:$TEST_DIR:localhost.crt}" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "listener.name.broker.ssl.keystore.key=\${dir:$TEST_DIR:localhost.key}" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "listener.name.broker.ssl.truststore.type=PEM" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "listener.name.broker.ssl.truststore.certificates=\${dir:$TEST_DIR:localhost.crt}" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties

  echo "listener.name.replication.ssl.keystore.type=PEM" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "listener.name.replication.ssl.keystore.certificate.chain=\${dir:$TEST_DIR:localhost.crt}" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "listener.name.replication.ssl.keystore.key=\${dir:$TEST_DIR:localhost.key}" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "listener.name.replication.ssl.truststore.type=PEM" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "listener.name.replication.ssl.truststore.certificates=\${dir:$TEST_DIR:localhost.crt}" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "ssl.endpoint.identification.algorithm=HTTPS" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties

  # Enable cluster mirroring in early access stage
  echo "unstable.api.versions.enable=true" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "unstable.feature.versions.enable=true" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties

  bin/kafka-storage.sh format -t "$cid" -c "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  KAFKA_HEAP_OPTS="-Xmx1G -Xms1G" JMX_PORT="${ref[jmx]}" LOG_DIR="$TEST_DIR/server"${ref[id]}"/logs" \
    KAFKA_LOG4J_OPTS="-Dlog4j2.configurationFile=file:$TEST_DIR/server"${ref[id]}"/config/log4j2.yaml" \
    bin/kafka-server-start.sh -daemon "$TEST_DIR"/server"${ref[id]}"/config/server.properties
}

test-teardown() {
  echo "Cleanup"
  pkill -SIGKILL -f "update.py" ||true
  pkill -SIGKILL -ef "kafka.tools" ||true
  pkill -SIGKILL -ef "kafka.Kafka" ||true
  pkill -SIGKILL -ef "quorum.QuorumPeerMain" ||true
}

test-setup() {
  test-teardown
  rm -rf "$TEST_DIR"
  download-kafka "$SRC_VERSION"
  ./gradlew jar
  create-crt "$TEST_DIR"
  create-jks "$TEST_DIR"

  echo "Creating properties files"
  echo "sasl.mechanism=PLAIN" > "$TEST_DIR"/kafka.properties
  echo "security.protocol=SASL_SSL" >> "$TEST_DIR"/kafka.properties
  echo 'sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \' >> "$TEST_DIR"/kafka.properties
  echo '  username="kafka" \' >> "$TEST_DIR"/kafka.properties
  echo '  password="changeit";' >> "$TEST_DIR"/kafka.properties
  echo "ssl.truststore.type=JKS" >> "$TEST_DIR"/kafka.properties
  echo "ssl.truststore.location=$TEST_DIR/truststore.jks" >> "$TEST_DIR"/kafka.properties
  echo "ssl.truststore.password=$PASSWD" >> "$TEST_DIR"/kafka.properties

  echo "sasl.mechanism=PLAIN" > "$TEST_DIR"/client.properties
  echo "security.protocol=SASL_SSL" >> "$TEST_DIR"/client.properties
  echo 'sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \' >> "$TEST_DIR"/client.properties
  echo '  username="client" \' >> "$TEST_DIR"/client.properties
  echo '  password="changeit";' >> "$TEST_DIR"/client.properties
  echo "ssl.truststore.type=JKS" >> "$TEST_DIR"/client.properties
  echo "ssl.truststore.location=$TEST_DIR/truststore.jks" >> "$TEST_DIR"/client.properties
  echo "ssl.truststore.password=$PASSWD" >> "$TEST_DIR"/client.properties

  echo "bootstrap.servers=$SRC_BOOTSTRAP" > "$TEST_DIR"/a-to-b.properties
  echo "sasl.mechanism=PLAIN" >> "$TEST_DIR"/a-to-b.properties
  echo "security.protocol=SASL_SSL" >> "$TEST_DIR"/a-to-b.properties
  echo 'sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \' >> "$TEST_DIR"/a-to-b.properties
  echo '  username="kafka" \' >> "$TEST_DIR"/a-to-b.properties
  echo '  password="changeit";' >> "$TEST_DIR"/a-to-b.properties
  echo "ssl.truststore.type=JKS" >> "$TEST_DIR"/a-to-b.properties
  echo "ssl.truststore.location=$TEST_DIR/truststore.jks" >> "$TEST_DIR"/a-to-b.properties
  echo "ssl.truststore.password=$PASSWD" >> "$TEST_DIR"/a-to-b.properties

  echo "Starting Kafka $SRC_VERSION (ZK) as source cluster"
  cd "$SRC_HOME"
  start-zserver 0 "$SRC_HOME"
  for i in 1 2; do start-broker-zk "$i" "localhost:${server0[rep]}" "$SRC_HOME"; done
  sleep 2
  bin/zookeeper-shell.sh localhost:2181 <<< "ls /brokers/ids" | tail -n1
  cd -

  echo "Starting local build (KRaft) as destination cluster"
  local dst_cid="$(bin/kafka-storage.sh random-uuid)" dst_boot="$(build-con-boot 3 3)"
  start-controller 3 "$dst_cid" "$dst_boot"
  for i in 4 5; do start-broker "$i" "$dst_cid" "$dst_boot"; done
  sleep 2
  bin/kafka-metadata-quorum.sh --bootstrap-server "$DST_BOOTSTRAP" --command-config "$TEST_DIR"/kafka.properties describe --re --hu

  # Enable cluster mirroring on destination if not enabled
  bin/kafka-features.sh --bootstrap-server "$DST_BOOTSTRAP" --command-config "$TEST_DIR"/kafka.properties upgrade --feature mirror.version=1

  sleep 2

  echo "Starting dashboard on http://127.0.0.1:8099"
  "$DASH_HOME"/update.py --kafka-home "$(pwd)" --bootstrap-server "$DST_BOOTSTRAP" \
    --command-config "$TEST_DIR"/kafka.properties > /tmp/update.log 2>&1 &
  disown

  echo "Creating topics with custom configs on source"
  "$SRC_HOME"/bin/kafka-topics.sh --bootstrap-server "$SRC_BOOTSTRAP" --create --topic my-topic-a \
    --partitions 3 --config retention.ms=86400000 --config max.message.bytes=2097152 \
    --command-config "$TEST_DIR"/kafka.properties
  "$SRC_HOME"/bin/kafka-topics.sh --bootstrap-server "$SRC_BOOTSTRAP" --create --topic my-topic-b \
    --partitions 5 --config retention.ms=172800000 --config max.message.bytes=1048576 \
    --command-config "$TEST_DIR"/kafka.properties
  "$SRC_HOME"/bin/kafka-topics.sh --bootstrap-server "$SRC_BOOTSTRAP" --create --topic new-topic-a \
    --partitions 2 --config retention.ms=604800000 --config max.message.bytes=4194304 \
    --command-config "$TEST_DIR"/kafka.properties

  echo "Granting client producer and consumer permissions on source"
  for topic in my-topic-a my-topic-b new-topic-a; do
    "$SRC_HOME"/bin/kafka-acls.sh --bootstrap-server "$SRC_BOOTSTRAP" --add --allow-principal User:client \
      --producer --topic "$topic" --command-config "$TEST_DIR"/kafka.properties
    "$SRC_HOME"/bin/kafka-acls.sh --bootstrap-server "$SRC_BOOTSTRAP" --add --allow-principal User:client \
      --consumer --topic "$topic" --group my-group --command-config "$TEST_DIR"/kafka.properties
  done

  sleep 2
}

test-setup

# STEP 1: List topics on both clusters
# Destination should be empty
echo "---- source ----"
"$SRC_HOME"/bin/kafka-topics.sh --bootstrap-server "$SRC_BOOTSTRAP" --list --command-config "$TEST_DIR"/kafka.properties
echo "---- destination ----"
bin/kafka-topics.sh --bootstrap-server "$DST_BOOTSTRAP" --list --command-config "$TEST_DIR"/kafka.properties

# STEP 2: Start 3 producers sending messages to all partitions
# and 2 consumers in my-group on source
# Messages flow continuously in the background
for topic in my-topic-a my-topic-b new-topic-a; do
  (i=0; while true; do echo "$((i++)):msg$i"; done) \
    | "$SRC_HOME"/bin/kafka-console-producer.sh --bootstrap-server "$SRC_BOOTSTRAP" --topic "$topic" \
    --property parse.key=true --property key.separator=: --producer.config "$TEST_DIR"/client.properties &
  disown
done
"$SRC_HOME"/bin/kafka-console-consumer.sh --bootstrap-server "$SRC_BOOTSTRAP" --include "my-topic-.*" \
  --group my-group --from-beginning --consumer.config "$TEST_DIR"/client.properties >/dev/null &
disown
"$SRC_HOME"/bin/kafka-console-consumer.sh --bootstrap-server "$SRC_BOOTSTRAP" --include "new-topic-a" \
  --group my-group --from-beginning --consumer.config "$TEST_DIR"/client.properties >/dev/null &
disown

# STEP 3: Create mirror 'a-to-b' and start mirroring topics matching 'my-topic.*'
# Expected: mirror link created and topics begin replicating
bin/kafka-cluster-mirrors.sh --bootstrap-server "$DST_BOOTSTRAP" --create --mirror a-to-b \
  --mirror-config "$TEST_DIR"/a-to-b.properties --command-config "$TEST_DIR"/kafka.properties
bin/kafka-cluster-mirrors.sh --bootstrap-server "$DST_BOOTSTRAP" --start --topics "my-topic.*" \
  --mirror a-to-b --command-config "$TEST_DIR"/kafka.properties

# STEP 4: Create mirror 'new-a-to-b' and start mirroring 'new-topic-a'
# Expected: second mirror link created and topic begins replicating
bin/kafka-cluster-mirrors.sh --bootstrap-server "$DST_BOOTSTRAP" --create --mirror new-a-to-b \
  --mirror-config "$TEST_DIR"/a-to-b.properties --command-config "$TEST_DIR"/kafka.properties
bin/kafka-cluster-mirrors.sh --bootstrap-server "$DST_BOOTSTRAP" --start --topics new-topic-a \
  --mirror new-a-to-b --command-config "$TEST_DIR"/kafka.properties

# STEP 5: Wait for all mirrors to reach MIRRORING state, then show their status
# Expected: all topic mirrors in MIRRORING state with active lag
wait-for-state "$DST_BOOTSTRAP" "MIRRORING" ".*" ".*" "$TEST_DIR"/kafka.properties
bin/kafka-cluster-mirrors.sh --bootstrap-server "$DST_BOOTSTRAP" --describe \
  --command-config "$TEST_DIR"/kafka.properties

# STEP 6: Stop all producers and consumers, then wait for replication lag
# to reach zero and metadata to sync
# Expected: all data fully replicated
pkill -SIGKILL -ef "ConsoleProducer" ||true
pkill -SIGKILL -ef "ConsoleConsumer" ||true
wait-for-lag-zero "$DST_BOOTSTRAP" ".*" ".*" "$TEST_DIR"/kafka.properties
wait-for-meta-sync "$DST_BOOTSTRAP" "$TEST_DIR"/kafka.properties

# STEP 7: Verify data integrity by comparing log dumps on my-topic-b partition 2
# Expected: last 5 records should be identical on both clusters
echo "---- source ----"
"$SRC_HOME"/bin/kafka-dump-log.sh --files "$TEST_DIR"/server1/data/my-topic-b-2/*.log --print-data-log 2>/dev/null | tail -5
echo "---- destination ----"
bin/kafka-dump-log.sh --files "$TEST_DIR"/server4/data/my-topic-b-2/*.log --print-data-log 2>/dev/null | tail -5

# STEP 8: Compare topic configuration on new-topic-a
# Expected: custom configs (retention.ms, max.message.bytes) should match on both clusters
echo "---- source ----"
"$SRC_HOME"/bin/kafka-configs.sh --bootstrap-server "$SRC_BOOTSTRAP" --entity-type topics --entity-name new-topic-a \
  --describe --command-config "$TEST_DIR"/kafka.properties
echo "---- destination ----"
bin/kafka-configs.sh --bootstrap-server "$DST_BOOTSTRAP" --entity-type topics --entity-name new-topic-a \
  --describe --command-config "$TEST_DIR"/kafka.properties

# STEP 9: Compare ACLs between clusters
# Expected: client producer and consumer permissions should be replicated from source to destination
echo "---- source ----"
"$SRC_HOME"/bin/kafka-acls.sh --bootstrap-server "$SRC_BOOTSTRAP" --list --command-config "$TEST_DIR"/kafka.properties
echo "---- destination ----"
bin/kafka-acls.sh --bootstrap-server "$DST_BOOTSTRAP" --list --command-config "$TEST_DIR"/kafka.properties

# STEP 10: Compare consumer group offsets for my-group
# Expected: offsets translated to destination
# Order may differ because older Kafka lacks topic+partition sorting
echo "---- source ----"
"$SRC_HOME"/bin/kafka-consumer-groups.sh --bootstrap-server "$SRC_BOOTSTRAP" --group my-group \
  --describe --command-config "$TEST_DIR"/kafka.properties
echo -e "\n---- destination ----"
bin/kafka-consumer-groups.sh --bootstrap-server "$DST_BOOTSTRAP" --group my-group \
  --describe --command-config "$TEST_DIR"/kafka.properties

# STEP 11: Migration complete, stop all mirrors to perform failover
# This produces a pid-reset control record for each mirrored partition
bin/kafka-cluster-mirrors.sh --bootstrap-server "$DST_BOOTSTRAP" --stop \
  --mirror a-to-b --topics ".*" --command-config "$TEST_DIR"/kafka.properties
bin/kafka-cluster-mirrors.sh --bootstrap-server "$DST_BOOTSTRAP" --stop \
  --mirror new-a-to-b --topics ".*" --command-config "$TEST_DIR"/kafka.properties
wait-for-state "$DST_BOOTSTRAP" "STOPPED" ".*" ".*" "$TEST_DIR"/kafka.properties
wait-for-meta-sync "$DST_BOOTSTRAP" "$TEST_DIR"/kafka.properties
bin/kafka-cluster-mirrors.sh --bootstrap-server "$DST_BOOTSTRAP" --describe \
  --command-config "$TEST_DIR"/kafka.properties

# STEP 12: Drain all remaining messages from destination using my-group
# Expected: total consumed equals the sum of lag values minus 10 pid-reset control records
# Final state should show zero lag
bin/kafka-console-consumer.sh --bootstrap-server "$DST_BOOTSTRAP" \
  --group my-group --include ".*" --timeout-ms 10000 \
  --consumer-property group.protocol=consumer \
  --consumer-property auto.commit.interval.ms=10 \
  --consumer.config "$TEST_DIR"/client.properties 2>&1 >/dev/null | grep "Processed"
bin/kafka-consumer-groups.sh --bootstrap-server "$DST_BOOTSTRAP" --group my-group \
  --describe --command-config "$TEST_DIR"/kafka.properties

# STEP 13: Cleanup all running processes
test-teardown
