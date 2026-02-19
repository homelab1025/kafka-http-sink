# KafkaHTTPSink
The overall goal of this service is to read from a kafka topic and push messages using the HTTP protocol to a partner.
The service is agnostic of the schema of the messages.

Yes, this is vibe coded and hand coded here and there, but to such a degree that it's hard to tell where the vibe code ends and the hand coded code begins.

## Modules
- a "kafka consumer" that is compatible with micronaut and that writes to a rocksdb database or to an in-memory location (based on configuration)
- a "pusher" that reads from the storage (either db or memory  - see above) and pushes the messages to the partner

## Protocol and contracts
The service needs to be agnostic of the underlying protocol. This supports the following protocols in this order of implementation:
1. pushing JSON messages over HTTP using the POST method
2. pushing JSON messages over a websocket that this service opens
3. pushing messages using protobuf, but remains to be seen whether it's gRPC that we are going to use

## Configuration options
- guaranteed delivery - if this is enabled, we will persists the messages into a temporary database and deliver them from there. If enabled, this comes with a retry mechanism that is configurable as well. If not enabled, then if the message is not delivered, it will be lost.
- batching - if this is enabled, we will batch messages before sending them to the partner. If enabled, we will also configure the batch size and timeout.

## Technical choices
- a rocksdb key value store that is used to persist messages when guaranteed delivery is enabled. Otherwise, a regular java queue that is read when pushing messages to the partner.
- we are relying on virtual threads when consuming and when pushing to the partner