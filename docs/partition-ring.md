# Partition Ring

The Partition Ring is an alternative to the standard Cortex ring that organizes ingesters into partitions. Each partition owns a set of tokens, and ingesters join partitions rather than directly owning tokens. This design creates a more stable and predictable distribution of data across ingesters, especially in multi-zone deployments.

## Key Benefits

1. **Zone Awareness**: Ingesters in different availability zones can be configured to be replicas of each other, improving resilience to zone failures.
2. **Reduced Resharding**: When ingesters join or leave the cluster, only the affected partitions need to be resharded, reducing the impact on the overall system.
3. **Predictable Token Distribution**: Tokens are assigned to partitions in a deterministic way, making the distribution more predictable.
4. **Improved Scaling**: Adding or removing ingesters has a more controlled impact on the system.

## Architecture

The partition ring consists of the following components:

1. **Partitions**: Each partition owns a set of tokens and has a list of ingesters that are members of the partition.
2. **Partition Ring**: A ring that maps tokens to partitions.
3. **Partition Lifecycler**: Manages the lifecycle of an ingester in the partition ring.
4. **Partition Ingester**: An ingester that uses the partition ring.
5. **Partition Distributor**: A distributor that uses the partition ring.

## Configuration

To use the partition ring, you need to configure both the ingester and distributor to use the partition-based implementations. Here's an example configuration:

```yaml
ingester:
  partition_ring:
    enabled: true
    ring:
      kvstore:
        store: consul
        prefix: partitions/
      replication_factor: 3
      zone_awareness_enabled: true
    num_partitions: 16
    partitions_per_ingester: 2
    partition_selection_strategy: zone-aware

distributor:
  partition_ring:
    enabled: true
    ring:
      kvstore:
        store: consul
        prefix: partitions/
      replication_factor: 3
      zone_awareness_enabled: true
    num_partitions: 16
```

See the [example configuration file](examples/partition-ring-config.yaml) for a complete example.

## How It Works

### Partition States

Partitions in the ring can be in one of three states:

1. **ACTIVE**: The partition is fully functional with the required number of ingesters across different AZs. Both read and write operations are allowed.
2. **READONLY**: The partition is in read-only mode, typically during scale-down operations. Write operations skip these partitions, but read operations continue.
3. **NON_READY**: The partition doesn't have enough ingesters to maintain the required replication factor. Both read and write operations will fail.

### Partition Selection

When an ingester starts, it selects which partitions to join based on the configured strategy:

1. **Zone-Aware**: The ingester selects partitions to ensure that each partition has at most one ingester per zone. This ensures that ingesters in different zones act as replicas of each other.
2. **Random**: The ingester selects partitions randomly.

### Data Distribution

When a write request arrives at the distributor:

1. The distributor calculates a hash for the tenant ID.
2. The hash is used to find the partition that owns the corresponding token.
3. The request is forwarded to all ingesters in that partition.

The key difference from the standard ring is that in the partition ring, all ingesters in a partition are replicas of each other. This means that a write request for a specific tenant will always go to the same set of ingesters, regardless of which series it contains. This provides better availability during AZ failures.

### Partition Lifecycle Management

The lifecycle of a partition involves several stages:

1. **Creation**: A new partition starts in NON_READY state. It transitions to ACTIVE only when it has the required number of ingesters across different AZs.
2. **Scale-Down**: When scaling down, a partition is marked as READONLY. Once all ingesters in the partition are empty, they can be safely removed.
3. **Removal**: When a partition no longer has enough ingesters, it transitions to NON_READY state and is eventually removed from the ring.

### Ingester Lifecycle

The lifecycle of an ingester in the partition ring is similar to the standard ring:

1. **PENDING**: The ingester is starting up and not yet ready to receive requests.
2. **JOINING**: The ingester is joining the ring and preparing to receive requests.
3. **ACTIVE**: The ingester is active and receiving requests.
4. **LEAVING**: The ingester is leaving the ring and no longer accepting write requests.

## Comparison with Standard Ring

| Feature | Standard Ring | Partition Ring |
|---------|--------------|----------------|
| Token Ownership | Ingesters own tokens directly | Partitions own tokens, ingesters join partitions |
| Zone Awareness | Ingesters from different zones own different tokens | Ingesters from different zones join the same partition |
| Resharding Impact | All ingesters affected | Only affected partitions |
| Scaling | Adding/removing ingesters affects the entire ring | More controlled impact |
| Configuration | Simpler | More complex |

## Implementation Details

The partition ring is implemented in the following files:

- `pkg/ring/partition_ring.go`: The main partition ring implementation.
- `pkg/ring/partition_ring_model.go`: The data model for the partition ring.
- `pkg/ring/partition_lifecycler.go`: The lifecycler for the partition ring.
- `pkg/ingester/partition_ring_integration.go`: Integration of the partition ring with the ingester.
- `pkg/distributor/partition_ring_integration.go`: Integration of the partition ring with the distributor.

## Future Improvements

1. **Dynamic Partition Adjustment**: Automatically adjust the number of partitions based on the cluster size.
2. **Partition Rebalancing**: Rebalance partitions when the cluster becomes unbalanced.
3. **Improved Zone Awareness**: Better algorithms for distributing partitions across zones.
4. **Partition Migration**: Support for migrating partitions between ingesters without downtime.
5. **Partition Splitting/Merging**: Support for splitting or merging partitions to better distribute load.## H
TTP API

The partition ring provides an HTTP API for managing partitions:

### List all partitions

```
GET /api/v1/partition/
```

Returns a JSON array of all partitions in the ring.

### Get a specific partition

```
GET /api/v1/partition/{partitionID}
```

Returns details about a specific partition.

### Delete a partition

```
DELETE /api/v1/partition/{partitionID}
```

Removes a partition from the ring.

### Mark a partition as read-only

```
POST /api/v1/partition/{partitionID}/readonly
```

Sets a partition to read-only mode, which is useful during scale-down operations.

### Mark a partition as active

```
POST /api/v1/partition/{partitionID}/active
```

Sets a partition to active mode, allowing both read and write operations.

### Get partition status

```
GET /api/v1/partition/{partitionID}/status
```

Returns detailed status information about a partition, including health and zone balance.