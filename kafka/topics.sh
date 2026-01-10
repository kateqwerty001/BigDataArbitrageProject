#!/bin/bash
BROKER=localhost:9092

TOPICS=(
  crypto.trades
  crypto.bbo
  crypto.depth
  crypto.deadletter
)

echo "Deleting topics (if they exist)..."
for t in "${TOPICS[@]}"; do
  /usr/local/kafka/bin/kafka-topics.sh \
    --bootstrap-server $BROKER \
    --delete \
    --topic $t 2>/dev/null
done

sleep 5

echo "Creating topics..."
for t in "${TOPICS[@]}"; do
  /usr/local/kafka/bin/kafka-topics.sh \
    --bootstrap-server $BROKER \
    --create \
    --topic $t \
    --partitions 1 \
    --replication-factor 1
done

echo "Done."

