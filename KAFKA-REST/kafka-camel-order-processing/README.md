# Kafka → Apache Camel → REST Order Integration

A GitHub-ready portfolio project demonstrating a production-style integration flow with Java 21, Spring Boot 3.5.6, Apache Camel 4.10.7, Kafka and PostgreSQL.

## Architecture

```text
Kafka: orders.in
      |
      v
Camel consumer
      |
      +--> JSON validation --------------------> orders.rejected
      |
      +--> persistent idempotency
      |
      +--> transform
      |
      v
bounded SEDA worker queue
      |
      | max configurable in-flight concurrency
      v
Downstream REST API
      |
      +--> 2xx -------------------------------> orders.success
      |
      +--> 4xx -------------------------------> orders.rejected
      |
      +--> 5xx / transient failure
               |
               +--> retry with exponential backoff
                         |
                         +--> success
                         |
                         +--> exhausted ------> orders.failed
```

## What this demonstrates

- Kafka consumer with multiple partitions
- Apache Camel routes
- JSON unmarshalling/marshalling
- Request validation
- Order transformation
- Bounded downstream concurrency
- REST API integration
- 2xx/4xx/5xx classification
- No retry for invalid/permanent 4xx requests
- Retry/backoff for transient 5xx failures
- Kafka failure topic after retry exhaustion
- Persistent PostgreSQL idempotency
- Docker Compose
- Maven
- GitHub Actions
- Local mock downstream API
- 2000-event load test

## Important: "1000 orders at a time"

The requirement is modeled as:

```properties
DOWNSTREAM_MAX_CONCURRENCY=1000
```

That means up to 1000 downstream calls can be in flight concurrently.

It does **not** mean 1000 requests per second.

In production, the value must be derived from the downstream contract, rate limits, connection pool, response time, CPU/memory, network capacity and load-test results. For an actual production deployment, start with a much smaller number and tune it.

Kafka partition count is a separate scaling dimension. This demo creates 20 partitions so Kafka consumer parallelism can scale independently from downstream concurrency.

## Result topics

### orders.success
Downstream returned HTTP 2xx.

### orders.rejected
Permanent failures:
- invalid Kafka payload
- invalid business validation
- downstream HTTP 4xx

These are not retried.

### orders.failed
Transient downstream failures after retry exhaustion:
- HTTP 5xx
- missing/failed HTTP response

## Idempotency

Kafka commonly gives at-least-once delivery. A crash can result in a duplicate event.

PostgreSQL provides:

```text
processed_orders(order_id PRIMARY KEY, status, ...)
```

The application atomically claims an order before calling the downstream service.

This is intentionally persistent rather than an in-memory `HashSet`.

Important: this demo should **not** be described as end-to-end exactly-once. Kafka, PostgreSQL and an external REST service cannot all participate in one transaction. The design is at-least-once + persistent idempotency + controlled retry + failure topic.

## Development and testing levels

### 1. Compile and unit test

```bash
mvn clean test
```

Expected:

```text
BUILD SUCCESS
```

### 2. Start infrastructure

```bash
docker compose up -d
docker compose ps
```

The stack contains Kafka, Zookeeper, PostgreSQL and a Kafka topic initializer.

Verify topics:

```bash
docker exec -it $(docker ps -qf "name=kafka")   kafka-topics --bootstrap-server kafka:29092 --list
```

Expected:

```text
orders.in
orders.success
orders.rejected
orders.failed
```

### 3. Start Spring Boot

```bash
mvn spring-boot:run
```

The application starts on port 8080.

The local downstream mock is:

```text
POST http://localhost:8080/mock/downstream/orders
```

### 4. Test success

Produce:

```bash
echo '{"orderId":"ORD-1","customerId":"C1","amount":25.50,"currency":"EUR","productCode":"PROD-1","quantity":1}' | docker exec -i $(docker ps -qf "name=kafka")   kafka-console-producer --bootstrap-server kafka:29092 --topic orders.in
```

Consume:

```bash
docker exec -it $(docker ps -qf "name=kafka")   kafka-console-consumer --bootstrap-server kafka:29092   --topic orders.success --from-beginning
```

Expected status:

```text
SUCCESS
```

### 5. Test application validation

Send quantity 4:

```json
{
  "orderId": "ORD-BAD",
  "customerId": "C1",
  "amount": 10.00,
  "currency": "EUR",
  "productCode": "PROD-1",
  "quantity": 4
}
```

Expected:

```text
orders.rejected
```

The downstream API is not called.

### 6. Test downstream 400

Set:

```text
productCode = INVALID
```

The mock returns HTTP 400.

Expected:

```text
orders.rejected
```

There is no retry.

### 7. Test downstream 503

Set:

```text
productCode = FAIL-503
```

The mock returns 503.

Default configuration:

```properties
RETRY_MAX_REDELIVERIES=3
RETRY_DELAY_MS=1000
```

The sequence is:

```text
initial request
    |
   503
    |
 retry 1
    |
   503
    |
 retry 2
    |
   503
    |
 retry 3
    |
   503
    |
orders.failed
```

This is initial attempt + three redeliveries.

### 8. Test downstream 500

Set:

```text
productCode = FAIL-500
```

Expected:
- retries
- final event on `orders.failed`
- database status `FAILED`

### 9. Test idempotency

Send the same `orderId` twice.

The first event is claimed and processed.

The second event is ignored because `processed_orders.order_id` is unique.

Inspect:

```bash
docker exec -it $(docker ps -qf "name=postgres")   psql -U orders -d orders   -c "select * from processed_orders order by created_at;"
```

### 10. Test 2000 orders

```bash
chmod +x infra/generate-2000-orders.sh
./infra/generate-2000-orders.sh
```

This publishes 2000 valid order events to `orders.in`.

Observe:

```bash
docker compose logs -f
```

The important behavior is:

```text
2000 Kafka events
       |
       v
bounded queue
       |
       v
configured downstream concurrency
```

There is no uncontrolled "spawn one thread per order" implementation.

## Credentials and secrets

No real credentials are stored in the repository.

Use environment variables:

```bash
export DB_USERNAME=orders
export DB_PASSWORD=your-local-password
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

Use `.env.example` as a template.

For Kubernetes/cloud environments, use Secrets or a proper secret manager. Never commit:
- passwords
- API tokens
- private keys
- certificates
- production URLs containing secrets

## Files

```text
.
├── .github/workflows/build.yml
├── infra/
│   ├── generate-2000-orders.sh
│   └── sample-orders.json
├── src/main/java/com/example/orderintegration/
│   ├── config/RestMockController.java
│   ├── model/
│   ├── processor/
│   ├── route/OrderProcessingRoute.java
│   └── service/
├── src/main/resources/
│   ├── application.properties
│   └── schema.sql
├── src/test/java/
├── .env.example
├── .gitignore
├── Dockerfile
├── docker-compose.yml
├── pom.xml
└── README.md
```

## Interview explanation

> Kafka receives order events. Camel consumes and validates them, then uses PostgreSQL for persistent idempotency. Valid orders are transformed into the downstream API contract and placed behind a bounded worker queue. The downstream concurrency is configurable so the REST service is not overwhelmed. HTTP 2xx is published to the success topic. HTTP 4xx is treated as a permanent rejection and is not retried. HTTP 5xx and transient transport failures are retried with exponential backoff. After retry exhaustion, the event is published to the failed topic with error metadata. Because Kafka is at-least-once, persistent idempotency is used rather than claiming end-to-end exactly-once semantics.

## GitHub

```bash
git init
git add .
git commit -m "Add Kafka Camel REST order integration"
git branch -M main
git remote add origin <YOUR_REPOSITORY_URL>
git push -u origin main
```
