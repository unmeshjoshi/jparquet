# JParque: A Java Parquet Storage Engine 😊

JParque is a custom implementation of the Apache Parquet columnar storage format in Java, designed for efficient data storage and retrieval. 🚀

[Setup](setup.md)

## Storage Engine Comparison

| Feature | B+ Tree (OLTP) | LSM Tree (OLTP) | Parquet (OLAP) |
|---------|----------------|-----------------|----------------|
| **Key Characteristics** | - All data records are stored in leaf nodes<br>- Internal nodes only store keys for navigation<br>- Leaf nodes are linked for efficient range scans<br>- Balanced structure ensures O(log n) operations | - Writes go to an in-memory buffer (memtable)<br>- Memtable is periodically flushed to disk as SSTable<br>- Background compaction merges SSTables<br>- Multiple levels with increasing sizes | - Data stored by columns rather than rows<br>- Hierarchical structure with row groups<br>- Built-in support for nested data<br>- Efficient encoding and compression per column |
| **Advantages** | - Excellent for range queries<br>- Good for point queries<br>- Supports efficient updates in-place<br>- Maintains sorted data | - Excellent write performance<br>- Sequential disk I/O for writes<br>- Good compression ratios<br>- Efficient space reclamation | - Excellent for analytical queries<br>- High compression ratios<br>- Efficient column pruning<br>- Schema evolution support |
| **Disadvantages** | - Higher write amplification due to node splits<br>- Not optimal for sequential writes<br>- Memory overhead for internal nodes<br>- Fragmentation over time | - Read amplification (checking multiple levels)<br>- Write amplification during compaction<br>- Complex compaction strategies<br>- Higher read latency for point queries | - Not suitable for random updates<br>- Higher latency for full row retrieval<br>- Complex file format<br>- Memory intensive for wide tables |

## Comparison Matrix

| Feature | B+ Tree | LSM Tree | Parquet |
|---------|---------|----------|---------|
| Primary Use Case | OLTP | OLTP | OLAP |
| Write Latency | 1-10ms | 0.1-1ms | 100ms-1s (batch) |
| Point Read Latency | 0.1-1ms | 1-10ms | 100ms+ |
| Range Scan (1000 rows) | 10-100ms | 50-200ms | 1-5ms per column |
| Storage Overhead | 2-3x data size | 1.5-2x data size | 0.1-0.4x data size |
| Write Amplification | 2-3x | 10-50x (with compaction) | 1x (append-only) |
| Read Amplification | 1-2x | 5-10x | 0.1-0.3x (column pruning) |
| Memory Footprint | High (cache) | Medium (memtable) | Low (streaming) |
| Compression Ratio | 1.5-2x | 2-3x | 10-20x |
| Update Support | Yes (in-place) | No (log-structured) | No (immutable) |
| Typical Use Cases | - RDBMS Indexes<br>- Primary Keys<br>- Unique Constraints | - Key-Value Stores<br>- Write-heavy OLTP<br>- Document DBs | - Data Warehouses<br>- Analytics<br>- BI Tools |

## Use Case Recommendations

| Use Case | B+ Tree | LSM Tree | Parquet |
|----------|---------|----------|---------|
| Balanced read/write performance | ✓ | | |
| Frequent random access | ✓ | | |
| Common range scans | ✓ | | |
| In-place updates required | ✓ | | |
| Critical write throughput | | ✓ | |
| Mostly append-only data | | ✓ | |
| Important space efficiency | | ✓ | |
| Trade read latency for write performance | | ✓ | |
| Primary workload is analytical queries | | | ✓ |
| Data read more than written | | | ✓ |
| High compression needed | | | ✓ |
| Schema evolution required | | | ✓ |

## JParque Implementation

JParque implements the Parquet format with the following features:
- Column-based storage with efficient compression
- Support for various data types and nested structures
- Schema evolution capabilities
- ACID transaction support (in progress)
- Efficient metadata handling

For more details on implementation and usage, see the [documentation](docs/parquet_internals.md). 📚

## Columnar Storage Examples

### Example 1: Sales Analytics
Consider a `SALES` table of a store with 1 billion records:

```sql
-- Query: Find average sales per store in 2024
SELECT store_id, AVG(price * quantity) as avg_sale
FROM sales
WHERE YEAR(date) = 2024
GROUP BY store_id;
```
#### Storage Comparison

| Storage Type | Columns Read | Total I/O |
|--------------|--------------|-----------|
| Row Storage  | ALL columns (≈100 bytes/row) | 100GB |
| Columnar Storage (Parquet) | store_id (8 bytes/row), price (8 bytes/row), quantity (4 bytes/row), date (4 bytes/row) | 24GB (76% reduction) 🎉 |

### Example 2: User Behavior Analysis
Consider a user events table with 10 billion records:
```sql
-- Find average session duration by country and device type
SELECT
   country,
   device_type,
   AVG(session_duration) as avg_duration
FROM user_events
WHERE
   timestamp >= '2024-01-01'
  AND timestamp < '2024-02-01'
GROUP BY country, device_type;
```

#### Storage Comparison

| Storage Type | Columns Read                                                                                   | Total I/O |
|--------------|------------------------------------------------------------------------------------------------|-----------|
| Row Storage  | All columns (≈400 bytes/row)                                                                   | 4TB |
| Columnar Storage | country (2 bytes), <br>device_type (20 bytes), <br>session_duration (4 bytes), <br>timestamp (8 bytes) | 340GB (91.5% reduction) 🎉 |

### Example 3: Time-Series Data
Consider an IoT sensor readings table:
```sql
CREATE TABLE sensor_readings (
                                sensor_id BIGINT,
                                timestamp TIMESTAMP,
                                temperature FLOAT,
                                humidity FLOAT,
                                pressure FLOAT,
                                battery_level FLOAT,
                                status VARCHAR(10),
                                location_id BIGINT,
                                firmware_version VARCHAR(20)
);
```

#### Columnar Advantages
1. **Compression**:
   - Temperature: Often similar values = high compression
   - Status: Few distinct values = dictionary encoding
   - Firmware_version: Repeated strings = dictionary encoding


2. **Query Optimization**:
```sql
-- Find average temperature by location when humidity > 80%
SELECT
   location_id,
   AVG(temperature)
FROM sensor_readings
WHERE
   humidity > 80
GROUP BY location_id;
```

3. **Parquet advantages**:
   * Predicate pushdown on humidity column
   * Reads only 3 columns instead of 9
   * Dictionary encoding for location_id
   * Efficient min/max statistics per column chunk

### Key Benefits Demonstrated
1. **I/O Reduction**:
   - Reads only required columns
   - Skips irrelevant row groups using statistics

2. **Compression Efficiency**:
   - Column-specific encoding
   - Better compression ratios for similar values
   - Dictionary encoding for low-cardinality columns

3. **Query Performance**:
   - Vectorized processing
   - Predicate pushdown
   - Parallel processing of columns

## Column Grouping Strategies

### Why Group Columns?
While Parquet is a columnar format, storing every column separately isn't always optimal. Column grouping (or column chunking) is crucial for:
1. Reducing I/O overhead for related columns
2. Optimizing for common query patterns
3. Balancing compression vs. access patterns

### Column Family Examples

#### Example 1: E-commerce Order Table
```sql
CREATE TABLE orders (
   -- Order Identification Group
                       order_id BIGINT,
                       order_date TIMESTAMP,
                       order_status VARCHAR(20),

   -- Customer Information Group
                       customer_id BIGINT,
                       customer_name VARCHAR(100),
                       customer_email VARCHAR(100),

   -- Shipping Information Group
                       shipping_address VARCHAR(200),
                       shipping_city VARCHAR(50),
                       shipping_country VARCHAR(2),
                       shipping_postal_code VARCHAR(10),

   -- Payment Information Group
                       payment_method VARCHAR(20),
                       card_last_four VARCHAR(4),
                       payment_status VARCHAR(20)
);
```

**Column Grouping Strategy:**
```json
{
   "column_groups": [
      {
         "name": "order_core",
         "columns": ["order_id", "order_date", "order_status"],
         "reason": "Always accessed together in order lookup queries"
      },
      {
         "name": "customer_info",
         "columns": ["customer_id", "customer_name", "customer_email"],
         "reason": "Customer profile queries typically need all customer fields"
      },
      {
         "name": "shipping_address",
         "columns": ["shipping_address", "shipping_city", "shipping_country", "shipping_postal_code"],
         "reason": "Shipping-related queries need full address"
      },
      {
         "name": "payment_info",
         "columns": ["payment_method", "card_last_four", "payment_status"],
         "reason": "Payment processing queries access these together"
      }
   ]
}
```

#### Example 2: IoT Sensor Readings
```sql
CREATE TABLE sensor_data (
   -- Identification Group
                            device_id BIGINT,
                            timestamp TIMESTAMP,
                            batch_id BIGINT,

   -- Environmental Metrics Group
                            temperature FLOAT,
                            humidity FLOAT,
                            pressure FLOAT,

   -- Device Status Group
                            battery_level FLOAT,
                            signal_strength INT,
                            firmware_version VARCHAR(20),

   -- Location Data Group
                            latitude DECIMAL(9,6),
                            longitude DECIMAL(9,6),
                            altitude FLOAT
);
```

**Column Grouping Strategy:**
```json
{
   "column_groups": [
      {
         "name": "time_series_key",
         "columns": ["device_id", "timestamp"],
         "reason": "Primary access pattern for time-series queries"
      },
      {
         "name": "environmental",
         "columns": ["temperature", "humidity", "pressure"],
         "reason": "Environmental metrics are typically analyzed together"
      },
      {
         "name": "device_status",
         "columns": ["battery_level", "signal_strength", "firmware_version"],
         "reason": "Device health monitoring queries"
      },
      {
         "name": "location",
         "columns": ["latitude", "longitude", "altitude"],
         "reason": "Spatial queries need all location components"
      }
   ]
}
```

### Column Grouping Best Practices

1. **Access Pattern Analysis**
   - Group columns that are frequently accessed together
   - Keep columns with similar query frequency in the same group
   - Separate columns with very different access patterns

2. **Data Characteristics**
   - Group columns with similar data types
   - Consider compression efficiency within groups
   - Keep high-cardinality and low-cardinality columns separate

3. **Size and Performance Balance**
   - Avoid groups that are too large (increases I/O for partial access)
   - Avoid groups that are too small (increases metadata overhead)
   - Consider the row group size in relation to column groups

4. **Query Optimization**
   - Enable efficient predicate pushdown
   - Optimize for common join patterns
   - Consider aggregation query patterns

5. **Storage Considerations**
   - Balance between compression ratio and access flexibility
   - Consider memory usage during reading/writing
   - Account for caching behavior
