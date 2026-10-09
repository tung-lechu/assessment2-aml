#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.sh"
KAFKA_HOME="$PROJECT_ROOT/.runtime/kafka_2.13-3.9.1"
CONFIG="$PROJECT_ROOT/.runtime/kafka-server.properties"
mkdir -p "$PROJECT_ROOT/.runtime/kafka-logs"
cat > "$CONFIG" <<EOF
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:9093
listeners=PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093
advertised.listeners=PLAINTEXT://127.0.0.1:9092
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
inter.broker.listener.name=PLAINTEXT
log.dirs=$PROJECT_ROOT/.runtime/kafka-data
num.partitions=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
auto.create.topics.enable=false
EOF
if [ ! -f "$PROJECT_ROOT/.runtime/kafka-data/meta.properties" ]; then
  CLUSTER_ID="$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)"
  "$KAFKA_HOME/bin/kafka-storage.sh" format -t "$CLUSTER_ID" -c "$CONFIG"
fi
export LOG_DIR="$PROJECT_ROOT/.runtime/kafka-logs"
export KAFKA_HEAP_OPTS="-Xms256m -Xmx512m"
echo "Starting local Kafka at 127.0.0.1:9092. Stop with Ctrl+C."
exec "$KAFKA_HOME/bin/kafka-server-start.sh" "$CONFIG"
