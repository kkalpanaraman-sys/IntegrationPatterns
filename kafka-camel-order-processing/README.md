# Kafka + Apache Camel Order Processing

A GitHub-ready Spring Boot integration project demonstrating:

- Kafka consuming order events
- Apache Camel routing
- Validation and transformation
- A downstream REST API that accepts at most 1,000 in-flight requests in this demo
- Downstream-owned duplicate protection using `Idempotency-Key`
- Retry handling for transient downstream failures
- No PostgreSQL and no database dependency
- Three Kafka result topics: success, rejected, failed
- Docker Compose for Kafka
- A mock downstream REST API for local testing
- Configurable downstream REST URL from environment variables
- OAuth 2.0 Client Credentials support with cached access tokens

## Architecture

```text
Kafka: orders.in
       |
       v
+-----------------------+
| Spring Boot + Camel   |
|                       |
|  1. Deserialize       |
|  2. Validate          |
|  3. Transform         |
|  4. Add Idempotency   |
|     Key = orderId     |
+-----------+-----------+
            |
            v
       SEDA queue
            |
            v
   Downstream REST API
            |
       +----+----+
       |         |
      2xx       4xx
       |         |
       v         v
orders.success orders.rejected

5xx / transient failure
        |
      retry
        |
   retries exhausted
        |
        v
 orders.failed
```

### Duplicate handling

The integration layer does **not** keep an idempotency database.

The order's stable `orderId` is sent as:

```http
Idempotency-Key: ORDER-1001
```

The mock downstream keeps track of processed keys to demonstrate the target-owned responsibility. In a real production system, the downstream would persist the idempotency key durably.

If the same order is delivered again by Kafka, the integration sends the same idempotency key. The target can return `ALREADY_PROCESSED` instead of performing the business operation again.

This project does not claim exactly-once processing across Kafka and an external REST API.

## Technology

- Java 21
- Spring Boot 3.5.6
- Apache Camel 4.10.7
- Apache Kafka
- Spring Security OAuth2 Client (Client Credentials grant)
- Maven
- Docker / Docker Compose

## Prerequisites

Install:

- Java 21
- Maven 3.9+
- Docker Desktop or Docker Engine with Compose
- Git

Check:

```bash
java -version
mvn -version
docker --version
docker compose version
```

## Step 1 - Clone the project

```bash
git clone <YOUR-GITHUB-REPOSITORY-URL>
cd kafka-camel-order-processing
```

## Step 2 - Start Kafka

```bash
docker compose up -d
```

Check containers:

```bash
docker compose ps
```

## Step 3 - Build the application

```bash
mvn clean test
```

Then package it:

```bash
mvn clean package
```

The JAR will be created under `target/`.

## Step 4 - Configure the downstream REST endpoint and OAuth 2.0

The REST endpoint is configurable; it is not hardcoded in the route anymore.

- Environment variable: `DOWNSTREAM_ENDPOINT_URL`
- Spring property: `downstream.endpoint-url`
- Used by: `OrderProcessingRoute` through Camel `toD(...)`

Copy the example configuration:

```bash
cp .env.example .env
```

For the included local mock, leave these settings as shown:

```dotenv
DOWNSTREAM_ENDPOINT_URL=http://localhost:8080/mock/downstream/orders
DOWNSTREAM_AUTH_ENABLED=false
```

The local mock does not require OAuth. To call a real OAuth 2.0-protected API, edit your local `.env` with the values supplied by that API's provider/identity team:

```dotenv
DOWNSTREAM_ENDPOINT_URL=https://api.example.com/v1/orders
DOWNSTREAM_AUTH_ENABLED=true
OAUTH2_TOKEN_URI=https://identity.example.com/oauth2/token
OAUTH2_CLIENT_ID=your-client-id
OAUTH2_CLIENT_SECRET=your-client-secret
OAUTH2_SCOPE=orders.write
OAUTH2_CLIENT_AUTH_METHOD=client_secret_basic
```

This project implements the **OAuth 2.0 Client Credentials grant**. Before each downstream call, the integration obtains an access token from the configured token endpoint (or reuses the cached token while it remains valid) and sends:

```http
Authorization: Bearer <access-token>
Idempotency-Key: <orderId>
Content-Type: application/json
```

`OAUTH2_CLIENT_AUTH_METHOD` supports `client_secret_basic` (default) and `client_secret_post`. Use the method required by your identity provider. The provider must issue a token suitable for the downstream API and the configured scope/audience. OAuth providers vary, so confirm their token URL, client authentication method, scopes, and any audience/resource parameter. This template does not add a custom `audience` parameter automatically.

**Secrets:** never commit `.env` or real client secrets. `.env` is ignored by Git. For deployment, inject secrets from your platform's secret manager/Kubernetes Secret rather than storing them in source control. The OAuth-enabled flow has not been tested against your real identity provider; you must configure provider-specific values.

To load the local `.env` into your shell on macOS/Linux and start the application:

```bash
set -a
. ./.env
set +a
mvn spring-boot:run
```

On PowerShell, set the environment variables in that terminal or use your IDE run configuration. Spring Boot does not automatically load a plain `.env` file by itself.

## Step 5 - Start the application

```bash
mvn spring-boot:run
```

The application starts on:

```text
http://localhost:8080
```

The same application also exposes the local mock downstream REST API.

## Step 6 - Understand the Kafka topics

The application uses:

```text
orders.in
orders.success
orders.rejected
orders.failed
```

The normal flow is:

```text
orders.in
   -> validation
   -> transformation
   -> downstream REST
   -> orders.success / orders.rejected
```

Temporary downstream failures are retried. If retries are exhausted, the order is published to `orders.failed`.

## Step 7 - Send one valid order

Create a file such as `order.json`:

```json
{
  "orderId": "ORDER-1001",
  "customerId": "CUST-001",
  "amount": 49.99,
  "currency": "EUR",
  "productCode": "BROADBAND",
  "quantity": 1
}
```

Produce it to Kafka using the Kafka container. The exact command can vary by Docker image, so first inspect the running Kafka container if needed:

```bash
docker compose ps
```

For the included Bitnami Kafka setup, you can use:

```bash
docker exec -i kafka bash -c 'echo '\''{"orderId":"ORDER-1001","customerId":"CUST-001","product":"BROADBAND","quantity":1,"amount":49.99}'\'' | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic orders.in'
```

Watch the application logs:

```text
Received order...
```

Then inspect the success topic using the Kafka console consumer inside the container.

## Step 8 - Test duplicate handling

Send the exact same order again:

```json
{
  "orderId": "ORDER-1001",
  "customerId": "CUST-001",
  "amount": 49.99,
  "currency": "EUR",
  "productCode": "BROADBAND",
  "quantity": 1
}
```

The integration sends:

```http
Idempotency-Key: ORDER-1001
```

The mock downstream sees that `ORDER-1001` was already processed and returns:

```json
{
  "orderId": "ORDER-1001",
  "status": "ALREADY_PROCESSED"
}
```

This demonstrates the **target-owned idempotency** model without PostgreSQL.

## Step 9 - Test validation failure

Send an invalid order, for example with a missing order ID or invalid quantity.

Expected result:

```text
orders.rejected
```

No downstream REST call should be made for a validation failure.

## Step 10 - Test downstream 4xx

Use the mock product `INVALID`:

```json
{
  "orderId": "ORDER-4001",
  "customerId": "CUST-001",
  "amount": 10.00,
  "currency": "EUR",
  "productCode": "INVALID",
  "quantity": 1
}
```

The mock REST API returns HTTP 400.

Expected result:

```text
orders.rejected
```

A 4xx business/client error should not be treated as a transient failure.

## Step 11 - Test downstream 5xx / retry

Use:

```json
{
  "orderId": "ORDER-5001",
  "customerId": "CUST-001",
  "amount": 10.00,
  "currency": "EUR",
  "productCode": "FAIL-500",
  "quantity": 1
}
```

The mock downstream returns HTTP 500.

The route retries according to:

```properties
retry.max-redeliveries=3
retry.delay-ms=1000
```

After retry exhaustion, the order is sent to:

```text
orders.failed
```

## Step 12 - Test 503

Use:

```json
{
  "orderId": "ORDER-5031",
  "customerId": "CUST-001",
  "amount": 10.00,
  "currency": "EUR",
  "productCode": "FAIL-503",
  "quantity": 1
}
```

This simulates a temporary downstream outage and exercises the retry path.

## Step 13 - Generate 2,000 orders

The repository contains:

```text
infra/generate-2000-orders.sh
```

Run:

```bash
chmod +x infra/generate-2000-orders.sh
./infra/generate-2000-orders.sh
```

The script generates a batch of test orders that can be produced into Kafka.

### Important: 1,000 is concurrency, not necessarily 1,000 requests/second

The property:

```properties
downstream.max-concurrency=1000
```

means the demo can create up to 1,000 concurrent downstream workers. It does **not** mean the downstream can process 1,000 requests every second.

In a real production system, this value should be tuned based on downstream capacity, latency, connection pools, CPU, memory, and the provider's actual limits. Starting lower is normally safer.

## Configuration

Main configuration is in:

```text
src/main/resources/application.properties
```

Important properties:

```properties
kafka.bootstrap-servers=localhost:9092
kafka.consumer.group-id=order-integration
kafka.consumer.consumers=10

downstream.endpoint-url=http://localhost:8080/mock/downstream/orders
downstream.auth-enabled=false
downstream.max-concurrency=10
downstream.queue-size=2000

oauth2.token-uri=
oauth2.client-id=
oauth2.client-secret=
oauth2.scope=
oauth2.client-auth-method=client_secret_basic

retry.max-redeliveries=3
retry.delay-ms=1000
```

Environment variables can override the values, for example:

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export DOWNSTREAM_ENDPOINT_URL=https://api.example.com/v1/orders
export DOWNSTREAM_AUTH_ENABLED=true
export OAUTH2_TOKEN_URI=https://identity.example.com/oauth2/token
export OAUTH2_CLIENT_ID=your-client-id
export OAUTH2_CLIENT_SECRET=your-client-secret
export OAUTH2_SCOPE=orders.write
export DOWNSTREAM_MAX_CONCURRENCY=10
export RETRY_MAX_REDELIVERIES=3
```

## Project structure

```text
kafka-camel-order-processing/
├── .github/
│   └── workflows/
│       └── build.yml
├── infra/
│   ├── generate-2000-orders.sh
│   └── sample-orders.json
├── src/
│   ├── main/
│   │   ├── java/com/example/orderintegration/
│   │   │   └── security/
│   │   │       ├── OAuth2ClientConfig.java
│   │   │       └── DownstreamOAuth2Processor.java
│   │   │   ├── config/
│   │   │   │   └── RestMockController.java
│   │   │   ├── model/
│   │   │   ├── processor/
│   │   │   ├── route/
│   │   │   │   └── OrderProcessingRoute.java
│   │   │   └── service/
│   │   │       ├── OrderTransformationService.java
│   │   │       └── OrderValidationService.java
│   │   └── resources/
│   │       └── application.properties
│   └── test/
├── .env.example
├── .gitignore
├── Dockerfile
├── docker-compose.yml
├── pom.xml
└── README.md
```

## Error-handling strategy

### Validation error

```text
Invalid input
    ↓
orders.rejected
```

### HTTP 4xx

```text
Downstream rejects request
    ↓
orders.rejected
```

### HTTP 5xx / transient failure

```text
Downstream failure
    ↓
retry
    ↓
retry
    ↓
retry
    ↓
orders.failed
```

### Duplicate order

```text
Kafka redelivery
    ↓
Same orderId
    ↓
Same Idempotency-Key
    ↓
Downstream recognizes duplicate
    ↓
No duplicate business operation
```

## Why there is no database

This project intentionally models a system where the downstream service owns duplicate protection.

The integration does not maintain its own idempotency table because that responsibility is already provided by the target system.

If a real target did **not** support idempotency, a persistent integration-side mechanism could be introduced as a separate design decision.

## GitHub setup

Create a new empty repository on GitHub, then from the project directory:

```bash
git init
git add .
git commit -m "Initial Kafka Camel order integration project"
git branch -M main
git remote add origin <YOUR-GITHUB-REPOSITORY-URL>
git push -u origin main
```

Before pushing, check:

```bash
git status
git diff --cached
```

Do not commit secrets. The `.gitignore` excludes local environment files such as `.env`.

## Recommended development sequence

If building this project yourself rather than just running the finished version:

1. Create the Spring Boot Maven project.
2. Add Apache Camel dependencies.
3. Add Kafka support.
4. Create `OrderEvent`.
5. Create validation service.
6. Create transformation service.
7. Create the Kafka input route.
8. Add the downstream REST call.
9. Add `Idempotency-Key = orderId`.
10. Add the mock downstream duplicate check.
11. Add success/rejected result topics.
12. Add retry handling for transient failures.
13. Add the failed topic.
14. Add bounded concurrency with SEDA.
15. Add unit tests.
16. Run the complete local test flow.
17. Commit to Git.
18. Push to GitHub.

This sequence makes each integration concept testable before adding the next one.

## Production considerations

This repository is a learning/portfolio project. A production implementation would additionally consider:

- durable downstream idempotency storage
- authentication and TLS
- secret management
- Kafka security
- schema/version management
- observability and correlation IDs
- metrics and alerting
- dead-letter recovery procedures
- graceful shutdown
- connection-pool limits
- downstream rate limits
- circuit breakers
- reconciliation for uncertain REST outcomes
- Kubernetes deployment and resource limits

## Interview explanation

A concise way to explain the duplicate design:

> Kafka provides at-least-once delivery, so duplicate delivery is possible. In this design, I don't maintain a separate idempotency database in the integration layer because the downstream system owns duplicate handling. I pass the stable order ID as an idempotency key. If Kafka redelivers the same order, the same key is sent again and the downstream system can recognize that it has already processed the business operation.

I would not claim exactly-once processing across Kafka and the external REST system.
