# Persistence Layer

The KafkaHTTPSink service provides three storage options for message persistence, each suitable for different use cases. The storage layer is abstracted through the `MessageStorage` interface, making it easy to switch between implementations via configuration.

## Storage Options

### 1. In-Memory Storage

**Implementation:** `InMemoryStorage`

**Use Case:** Development, testing, or high-throughput scenarios where message loss is acceptable.

**Characteristics:**
- Uses `ConcurrentLinkedQueue` for thread-safe message queuing
- Uses `ConcurrentHashMap` for fast message lookup by ID
- Zero persistence - all messages are lost on application restart
- Minimal latency
- No external dependencies

**Configuration:**
```properties
kafka.sink.storage.type=memory
```

**When to use:**
- Development and testing environments
- High-throughput scenarios where guaranteed delivery is not required
- When message loss on restart is acceptable
- When you want minimal operational overhead

**Limitations:**
- No durability - messages lost on crash or restart
- Memory-bound - storage limited by available RAM
- Not suitable for production workloads requiring guaranteed delivery

---

### 2. RocksDB Storage

**Implementation:** `RocksDBStorage`

**Use Case:** Single-node production deployments requiring guaranteed delivery with local persistence.

**Characteristics:**
- Embedded key-value store (no separate server needed)
- Disk-backed persistence
- High performance with LSM-tree architecture
- Low operational complexity
- Single-node only (not distributed)

**Architecture:**
- Uses RocksDB as an embedded database library
- Messages are serialized to JSON using Jackson `ObjectMapper`
- Message IDs are used as keys (UTF-8 encoded)
- Message objects are serialized as values
- Database files stored on local disk

**Configuration:**
```properties
kafka.sink.storage.type=rocksdb
kafka.sink.storage.rocksdb.path=/var/lib/kafka-sink/rocksdb
```

**When to use:**
- Production deployments on a single node
- When you need guaranteed delivery with persistent storage
- When you want simple operations (no cluster to manage)
- When write throughput is moderate (thousands of messages/second)
- When you have sufficient local disk space

**Operational Considerations:**
- **Disk Space:** Monitor disk usage at the configured path
- **Backups:** The database path should be included in backup strategy
- **Performance:** Write performance depends on disk I/O
- **Recovery:** On restart, all unprocessed messages are recovered from disk
- **Compaction:** RocksDB handles compaction automatically, but may cause brief performance spikes

**Limitations:**
- Single-node only - not distributed
- Horizontal scaling requires application-level partitioning
- Backup/recovery is file-based
- No built-in replication

---

### 3. FoundationDB Storage

**Implementation:** `FoundationDBStorage`

**Use Case:** Distributed production deployments requiring strong consistency, high availability, and guaranteed delivery across multiple nodes.

**Characteristics:**
- Distributed, ACID-compliant key-value store
- Multi-node deployment with automatic replication
- Strong consistency guarantees
- Horizontal scalability
- High availability with automatic failover

**Architecture:**
- Uses FoundationDB client library to connect to FDB cluster
- Messages stored in a dedicated directory subspace (`kafka-sink-messages` by default)
- Message IDs are packed as Tuples for efficient key encoding
- Message objects are serialized to JSON using Jackson `ObjectMapper`
- Transactions ensure atomic operations
- Directory Layer provides namespace isolation

**Configuration:**
```properties
kafka.sink.storage.type=foundationdb
kafka.sink.storage.foundationdb.api-version=730
kafka.sink.storage.foundationdb.cluster-file=/etc/foundationdb/fdb.cluster
kafka.sink.storage.foundationdb.directory=kafka-sink-messages
```

**Configuration Parameters:**
- `api-version`: FoundationDB API version (default: 730 for version 7.3.x)
- `cluster-file`: Path to FDB cluster file (optional, uses default if not specified)
- `directory`: Directory name for message storage (default: `kafka-sink-messages`)

**When to use:**
- Large-scale production deployments
- Multi-node/distributed architectures
- When you need strong consistency guarantees
- When high availability is critical
- When horizontal scaling is required
- When write throughput exceeds single-node capacity

**Operational Considerations:**

**Prerequisites:**
- FoundationDB cluster must be installed and running
- FoundationDB client library must be installed on application host
- Cluster file (`fdb.cluster`) must be accessible to the application

**Performance:**
- Read/write performance scales with cluster size
- Typical latency: 1-10ms for operations
- Throughput: tens of thousands of transactions per second per cluster
- Performance depends on cluster configuration and replication factor

**High Availability:**
- Automatic failover when nodes fail
- Configurable replication (typically 3x)
- No single point of failure
- Self-healing cluster

**Monitoring:**
- Use `fdbcli` to monitor cluster health
- Monitor transaction latency and throughput
- Watch for storage space across cluster nodes
- Monitor replication lag

**Backup & Recovery:**
- FoundationDB provides built-in continuous backup
- Point-in-time recovery supported
- Backups can be restored to new clusters
- Use `fdbbackup` and `fdbrestore` utilities

**Scaling:**
- Add more nodes to increase capacity and throughput
- Messages automatically rebalanced across cluster
- No application changes needed for horizontal scaling

**Directory Isolation:**
- Each instance can use a separate directory for isolation
- Multiple applications can safely share the same FDB cluster
- Directory structure: `["kafka-sink-messages"]` by default

**Limitations:**
- Requires separate FoundationDB cluster infrastructure
- More complex operations compared to embedded solutions
- Value size limit: 100KB per key (messages must fit within this)
- Higher operational overhead

---

## Choosing a Storage Option

| Criteria | In-Memory | RocksDB | FoundationDB |
|----------|-----------|---------|--------------|
| **Durability** | None | Disk-backed | Replicated across cluster |
| **Guaranteed Delivery** | No | Yes (single node) | Yes (distributed) |
| **High Availability** | No | No | Yes |
| **Horizontal Scaling** | No | No | Yes |
| **Operational Complexity** | Minimal | Low | Moderate |
| **Infrastructure Required** | None | Local disk | FDB cluster |
| **Best For** | Dev/Test | Single-node prod | Multi-node prod |

## Storage Configuration Examples

### Development Setup (In-Memory)
```properties
kafka.sink.storage.type=memory
kafka.sink.delivery.guaranteed=false
```

### Single-Node Production (RocksDB)
```properties
kafka.sink.storage.type=rocksdb
kafka.sink.storage.rocksdb.path=/var/lib/kafka-sink/rocksdb
kafka.sink.delivery.guaranteed=true
kafka.sink.retry.max-attempts=5
kafka.sink.retry.delay-ms=2000
```

### Multi-Node Production (FoundationDB)
```properties
kafka.sink.storage.type=foundationdb
kafka.sink.storage.foundationdb.api-version=730
kafka.sink.storage.foundationdb.cluster-file=/etc/foundationdb/fdb.cluster
kafka.sink.storage.foundationdb.directory=kafka-sink-messages
kafka.sink.delivery.guaranteed=true
kafka.sink.retry.max-attempts=5
kafka.sink.retry.delay-ms=2000
```

## Implementation Details

All storage implementations provide the same interface:

```java
public interface MessageStorage {
    void store(Message message);
    List<Message> retrieve(int batchSize);
    void delete(String messageId);
    void markAsFailed(Message message);
    long size();
    void close();
}
```

The storage implementation is automatically selected at runtime based on the `kafka.sink.storage.type` configuration property using Micronaut's `@Requires` annotations.

## Migration Between Storage Types

To migrate between storage types:

1. Ensure all messages in the current storage are processed
2. Stop the application
3. Update the `kafka.sink.storage.type` configuration
4. Configure the new storage-specific properties
5. Restart the application

Note: There is no automated migration tool. Messages in the old storage will not be automatically transferred to the new storage.
