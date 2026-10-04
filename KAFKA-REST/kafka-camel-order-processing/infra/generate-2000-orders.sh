#!/usr/bin/env bash
set -euo pipefail
BROKER="${KAFKA_BROKER:-localhost:9092}"
for i in $(seq 1 2000); do
  printf '{"orderId":"ORD-%04d","customerId":"CUST-%04d","amount":%d.99,"currency":"EUR","productCode":"PROD-%d","quantity":1}\n'     "$i" "$i" "$(( (i % 100) + 1 ))" "$(( (i % 20) + 1 ))"
done | kafka-console-producer --bootstrap-server "$BROKER" --topic orders.in
