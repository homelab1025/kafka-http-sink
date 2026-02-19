# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Run

```bash
./gradlew build          # compile + test
./gradlew run            # run locally (requires Kafka on localhost:9092)
./gradlew test           # run all tests
./gradlew shadowJar      # produce fat JAR
```

Java 25 is required (source and target compatibility set to 25).

## Architecture

The service reads from a Kafka topic and relays messages to a partner over HTTP POST. It is schema-agnostic — payloads are treated as opaque strings.

### Two delivery modes (controlled by `kafka.sink.async-delivery`)

**Sync / direct (default, `async-delivery=false`):**
```
Kafka → KafkaMessageConsumer → MessagePusher → HTTP endpoint
```
Offset is committed after `consume()` returns. If the push fails the batch is lost (no retry).

**Async / store-and-forward (`async-delivery=true`):**
```
Kafka → KafkaMessageConsumer → MessageStorage
                                     ↑ polled every 1 s
                               PusherService → MessagePusher → HTTP endpoint
```
`PusherService` is only instantiated when `async-delivery=true` (gated via `@Requires`). Failed pushes are retried up to `retry.max-attempts` times; messages exceeding the limit are dropped.

### Key abstractions

| Interface | Implementations | Selected via |
|---|---|---|
| `MessageStorage` | `InMemoryStorage`, `RocksDBStorage`, `FoundationDBStorage` | `kafka.sink.storage.type` (memory / rocksdb / foundationdb) |
| `MessagePusher` | `HttpPostPusher` | only implementation today |

Each storage implementation is gated with `@Requires(property = "kafka.sink.storage.type", value = "...")`.
`PusherService` is gated with `@Requires(property = "kafka.sink.async-delivery", value = "true")`.

## Coding
- prefer returning Optional instead of null values
- prefer using try-with-resources for resource management
- commit messages focus on explaining functionality