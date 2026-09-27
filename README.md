# kafkamock

In-memory mock of a Kafka broker, built with Spring Boot. Simulates topics, partitions, producers, consumer groups, offsets, and replay — all in-process, no real Kafka needed.

## Stack

- Java 21
- Spring Boot 4.0.1 (spring-boot-starter-webmvc)
- Lombok

## Structure

```
src/main/java/com/tekion/kafkamock/
├── KafkamockApplication.java        # entry point
├── BackgroundConsumersRunner.java   # bootstraps "orders" topic + 2 background consumers on startup
├── controller/
│   ├── ProducerController.java      # REST API: create topic, send messages
│   └── ConsumerController.java      # poll/reset endpoints (currently commented out)
├── service/
│   ├── BrokerService.java           # topic/partition registry, produce/fetch
│   ├── ConsumerService.java         # poll + offset tracking per consumer group
│   └── GroupCoordinator.java        # group membership, round-robin partition assignment
├── repository/
│   └── TopicPartition.java          # per-partition message log (append/read/trim)
└── model/
    └── Message.java                 # key, value, timestamp, offset
```

## Running

```
./mvnw spring-boot:run
```

On startup, topic `orders` (4 partitions) is created and two background consumers (`c1`, `c2`) in group `g1` start polling.

## API

| Method | Path                | Params                       | Description          |
|--------|----------------------|-------------------------------|-----------------------|
| POST   | `/api/producer/setup` | `topic`, `partitions`         | Create a topic        |
| POST   | `/api/producer/send`  | `topic`, `key`, `value`       | Produce a message     |

`ConsumerController` (`/api/consumer/poll`, `/api/consumer/reset`) is present but commented out — background polling in `BackgroundConsumersRunner` covers consumption for now.

## Notes

- State is in-memory only — restarting the app wipes all topics/messages/offsets.
- Partition assignment is round-robin by sorted consumer name within a group.
- `TopicPartition.trim()` exists for retention but isn't wired up anywhere yet.