#!/usr/bin/env bash
set -euo pipefail

# Publishes 2,000 demo orders through the Kafka container.
# Start the stack first with: docker compose up -d

for i in $(seq 1 2000); do
  printf '{"orderId":"ORD-%04d","customerId":"CUST-%04d","amount":%d.99,"currency":"EUR","productCode":"PROD-%d","quantity":1}\n' \
    "$i" "$i" "$(( (i % 100) + 1 ))" "$(( (i % 20) + 1 ))"
done | docker compose exec -T kafka kafka-console-producer --bootstrap-server kafka:29092 --topic orders.in
