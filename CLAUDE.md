# MessiSQSProvider

## Purpose
AWS SQS provider implementation for the Messi messaging and streaming abstraction layer. Enables applications using the Messi SDK to produce and consume messages via Amazon Simple Queue Service.

## Tech Stack
- Language: Java 11+
- Framework: None (library)
- Build: Maven
- Key dependencies: Messi SDK, AWS SQS SDK, Protocol Buffers

## Architecture
Provider library implementing the Messi SDK interfaces for Amazon SQS. Uses the provider pattern (Java SPI) so applications can use SQS as a message queue backend. Messages are serialized using Protocol Buffers. Suitable for point-to-point messaging and work queue patterns.

## Key Entry Points
- Messi SQS provider implementation classes
- `pom.xml` - Maven coordinates: `no.cantara.messi:messi-sqs-provider`

## Development
```bash
# Build
mvn clean install

# Test
mvn test
```

## Domain Context
Cloud messaging infrastructure. Part of the Messi messaging abstraction ecosystem, providing AWS SQS as a queue-based messaging backend for decoupled service communication.
