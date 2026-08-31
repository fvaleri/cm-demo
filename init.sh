#!/usr/bin/env bash

# Stop if not interactive mode
[[ $- != *i* ]] && echo "Usage: source init.sh" && exit 1

POC_BRANCH="cm-20260831"
TEST_DIR="/tmp/kafka-test"; mkdir -p "$TEST_DIR"
declare -A server0=([id]="0" [rep]="2181" [jmx]="7000" [fol]="5000" [oth]="6000")
declare -A server1=([id]="1" [rep]="6001" [jmx]="7001" [cli]="9091")
declare -A server2=([id]="2" [rep]="6002" [jmx]="7002" [cli]="9092")
declare -A server3=([id]="3" [rep]="6003" [jmx]="7003")
declare -A server4=([id]="4" [rep]="6004" [jmx]="7004" [cli]="9094")
declare -A server5=([id]="5" [rep]="6005" [jmx]="7005" [cli]="9095")
SRC_BOOTSTRAP="localhost:${server1[cli]},localhost:${server2[cli]}"
SRC_HOME="/tmp/kafka-src"
DST_BOOTSTRAP="localhost:${server4[cli]},localhost:${server5[cli]}"
DST_HOME="/tmp/kafka-dst"

get-src-kafka() {
  local dir="$1"
  if [[ ! "$dir" ]]; then
    echo "Usage: get-src-kafka <target_path>" >&2
    return 1
  fi
  if [[ ! -d "$dir" ]]; then
    mkdir -p "$dir"
    local url="https://archive.apache.org/dist/kafka/3.9.2/kafka_2.13-3.9.2.tgz"
    echo "Downloading and extracting Kafka 3.9.2"
    curl -L "$url" | tar -xz -C "$dir" --strip-components=1
  fi
}

build-dst-kafka() {
  local dir="$1"
  if [[ ! "$dir" ]]; then
    echo "Usage: build-dst-kafka <target_path>" >&2
    return 1
  fi
  if [[ ! -d "$dir" ]]; then
    local build_dir="$dir-build"
    mkdir -p "$build_dir"
    cd "$build_dir"
    echo "Cloning Kafka 4.2.0 from POC branch"
    git clone --depth 1 --branch "$POC_BRANCH" https://github.com/showuon/kafka.git .
    echo "Building Kafka release"
    ./gradlew clean releaseTarGz
    mkdir -p "$dir"
    tar -xzf core/build/distributions/kafka_2.13-4.2.0-SNAPSHOT.tgz -C "$dir" --strip-components=1
    cd -
  fi
}

build-con-boot() {
  local start="$1" end="$2"
  local boot=""
  for i in $(seq "$start" "$end"); do
    declare -n ref="server$i"
    boot+="${boot:+,}localhost:${ref[rep]}"
  done
  echo "$boot"
}

wait-for-meta-sync() {
  local bootstrap="$1" cmd_config="${2-}"
  local cmd_opts=""
  [[ -n "$cmd_config" ]] && cmd_opts="--command-config $cmd_config"
  local interval_ms
  interval_ms=$("$DST_HOME"/bin/kafka-configs.sh --bootstrap-server "$bootstrap" --entity-type brokers --all --describe $cmd_opts 2>/dev/null \
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
    "$DST_HOME"/bin/kafka-cluster-mirrors.sh --bootstrap-server "$bootstrap" --describe --json $cmd_opts 2>/dev/null | \
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
    "$DST_HOME"/bin/kafka-cluster-mirrors.sh --bootstrap-server "$bootstrap" --describe --json $cmd_opts 2>/dev/null | \
      jq -e --arg m "$mirror_re" --arg t "$topic_re" \
        '[.[] | select((.mirror | test($m)) and (.topic | test($t)))] | length > 0 and all(.state == "MIRRORING" and (.sourceOffset - .destinationOffset) == 0)' &>/dev/null && return 0
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
  cp "$DST_HOME"/config/log4j2.yaml "$TEST_DIR"/server"${ref[id]}"/config/log4j2.yaml

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

  "$DST_HOME"/bin/kafka-storage.sh format -t "$cid" -c "$TEST_DIR"/server"${ref[id]}"/config/server.properties -s
  KAFKA_HEAP_OPTS="-Xmx1G -Xms1G" JMX_PORT="${ref[jmx]}" LOG_DIR="$TEST_DIR/server"${ref[id]}"/logs" \
    KAFKA_LOG4J_OPTS="-Dlog4j2.configurationFile=file:$TEST_DIR/server"${ref[id]}"/config/log4j2.yaml" \
    "$DST_HOME"/bin/kafka-server-start.sh -daemon "$TEST_DIR"/server"${ref[id]}"/config/server.properties
}

# Destination cluster broker (uses local build, PEM for SSL)
start-broker() {
  local id="$1" cid="$2" boot="$3"
  declare -n ref="server$id"
  mkdir -p "$TEST_DIR"/server"${ref[id]}"/config "$TEST_DIR"/server"${ref[id]}"/logs "$TEST_DIR"/server"${ref[id]}"/data
  cp "$DST_HOME"/config/log4j2.yaml "$TEST_DIR"/server"${ref[id]}"/config/log4j2.yaml

  echo "process.roles=broker" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "node.id=${ref[id]}" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "log.dirs=$TEST_DIR/server${ref[id]}/data" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "controller.quorum.bootstrap.servers=$boot" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "auto.create.topics.enable=false" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties

  # Mirror tuning for faster replication
  echo "mirror.coordinator.threads=3" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  echo "mirror.num.replica.fetchers=3" >> "$TEST_DIR"/server"${ref[id]}"/config/server.properties
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

  "$DST_HOME"/bin/kafka-storage.sh format -t "$cid" -c "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  KAFKA_HEAP_OPTS="-Xmx1G -Xms1G" JMX_PORT="${ref[jmx]}" LOG_DIR="$TEST_DIR/server"${ref[id]}"/logs" \
    KAFKA_LOG4J_OPTS="-Dlog4j2.configurationFile=file:$TEST_DIR/server"${ref[id]}"/config/log4j2.yaml" \
    "$DST_HOME"/bin/kafka-server-start.sh -daemon "$TEST_DIR"/server"${ref[id]}"/config/server.properties
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
  # Removing previous test files here to allow debugging after teardown
  rm -rf "$TEST_DIR"
  get-src-kafka "$SRC_HOME"
  build-dst-kafka "$DST_HOME"
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

  # Fetch tuning for faster mirroring
  echo "fetch.max.bytes=10485760" >> "$TEST_DIR"/a-to-b.properties
  echo "fetch.response.max.bytes=52428800" >> "$TEST_DIR"/a-to-b.properties
  echo "fetch.wait.max.ms=500" >> "$TEST_DIR"/a-to-b.properties
  echo "socket.receive.buffer.bytes=1048576" >> "$TEST_DIR"/a-to-b.properties

  echo "Starting Kafka 3.9.2 (ZK) as source cluster"
  cd "$SRC_HOME"
  start-zserver 0 "$SRC_HOME"
  for i in 1 2; do start-broker-zk "$i" "localhost:${server0[rep]}" "$SRC_HOME"; done
  sleep 2
  bin/zookeeper-shell.sh localhost:2181 <<< "ls /brokers/ids" | tail -n1
  cd -

  echo "Starting POC Kafka (KRaft) as destination cluster"
  local dst_cid="$("$DST_HOME"/bin/kafka-storage.sh random-uuid)" dst_boot="$(build-con-boot 3 3)"
  start-controller 3 "$dst_cid" "$dst_boot"
  for i in 4 5; do start-broker "$i" "$dst_cid" "$dst_boot"; done
  sleep 2
  "$DST_HOME"/bin/kafka-metadata-quorum.sh --bootstrap-server "$DST_BOOTSTRAP" --command-config "$TEST_DIR"/kafka.properties describe --re --hu

  # Enable cluster mirroring on destination if not enabled
  "$DST_HOME"/bin/kafka-features.sh --bootstrap-server "$DST_BOOTSTRAP" --command-config "$TEST_DIR"/kafka.properties upgrade --feature mirror.version=1

  sleep 2

  echo "Starting dashboard on http://localhost:8099"
  dash/update.py --kafka-home "$DST_HOME" --bootstrap-server "$DST_BOOTSTRAP" \
    --command-config "$TEST_DIR"/kafka.properties > /tmp/dash.log 2>&1 &
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
