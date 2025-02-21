# Columnar Storage Implementations: OLTP vs OLAP

This document compares different columnar storage implementations, focusing on how they organize data and their performance characteristics.

## 1. Physical vs Logical Column Organization

### OLTP (Column Families - e.g., Cassandra)
```plaintext
/data/
├── users/
│   ├── profile_cf/          # Column Family 1
│   │   ├── 000001.sst      # Contains profile data
│   │   └── 000002.sst
│   └── activity_cf/         # Column Family 2
│       ├── 000001.sst      # Contains activity data
│       └── 000002.sst
└── metadata/
```

### OLAP (Parquet)
```plaintext
/data/
└── users.parquet           # Single file containing all columns
    ├── Row Group 1 (128MB)
    │   ├── Column: id
    │   ├── Column: name
    │   └── Column: email
    └── Row Group 2 (128MB)
        ├── Column: id
        ├── Column: name
        └── Column: email
```

## 2. Performance Characteristics

### Example Dataset
- 1 billion user records
- Total size: 1TB uncompressed
- Schema:
  ```sql
  CREATE TABLE users (
    id BIGINT,
    name VARCHAR,
    email VARCHAR,
    last_login TIMESTAMP,
    preferences JSON
  )
  ```

### 2.1 Storage Efficiency

#### Cassandra (Column Family)
- Raw size: 1TB
- Size with replication (RF=3): 3TB
- Size per column family:
  ```
  profile_cf:    800GB (id, name, email)
  activity_cf:   400GB (id, last_login)
  settings_cf:   1.8TB (id, preferences)
  ```

#### Parquet
- Raw size: 1TB
- Size after compression: ~300GB
  ```
  Column chunks:
  - id:          40GB  (high compression, sorted integers)
  - name:        100GB (medium compression, text)
  - email:       80GB  (medium compression, text)
  - last_login:  30GB  (high compression, timestamps)
  - preferences: 50GB  (medium compression, JSON)
  ```

### 2.2 Query Performance

#### Query 1: Get all user profile data
```sql
SELECT name, email FROM users WHERE id = ?
```

| Storage Type | Avg Latency | Notes |
|-------------|-------------|--------|
| Cassandra   | 5ms        | Fast because entire column family is in one file |
| Parquet     | 100ms      | Slower because needs to read from large file |

#### Query 2: Analytical query on login patterns
```sql
SELECT DATE_TRUNC('day', last_login) as day, 
       COUNT(*) as login_count 
FROM users 
GROUP BY DATE_TRUNC('day', last_login)
```

| Storage Type | Processing Time | Notes |
|-------------|----------------|--------|
| Cassandra   | 45 minutes    | Must scan all activity_cf files |
| Parquet     | 2 minutes     | Only reads last_login column, uses statistics |

## 3. Use Case Optimization

### 3.1 Write Performance (1 million records)

| Operation | Cassandra | Parquet |
|-----------|-----------|---------|
| Insert    | 500μs/row | N/A (batch only) |
| Update    | 500μs/row | N/A (immutable) |
| Delete    | 500μs/row | N/A (immutable) |
| Batch Write| 5s/million rows | 15s/million rows |

### 3.2 Read Performance

#### Random Access (1000 records)
```sql
SELECT * FROM users WHERE id IN (?, ?, ...)
```

| Storage | Time | IO Operations |
|---------|------|---------------|
| Cassandra | 50ms | ~1000 (can be parallel) |
| Parquet   | 2s   | ~10 (row group scans) |

#### Sequential Scan (specific columns)
```sql
SELECT email FROM users WHERE signup_date > '2024-01-01'
```

| Storage | Time | Data Read |
|---------|------|-----------|
| Cassandra | 30min | 800GB (entire profile_cf) |
| Parquet   | 45s   | 80GB (email column only) |

## 4. Memory Usage Patterns

### 4.1 Operational Workload (1000 concurrent users)

#### Cassandra
- Memory per query: ~100KB
- Cache efficiency: High for hot rows
- Total memory needed: 2-4GB

#### Parquet
- Memory per query: 1-128MB (row group size)
- Cache efficiency: High for columnar scans
- Total memory needed: 10-20GB

### 4.2 Analytical Workload

#### Cassandra
- Memory per query: 1-2GB
- Spill to disk: Frequent
- Temp space needed: 100-200GB

#### Parquet
- Memory per query: 100-200MB
- Spill to disk: Rare
- Temp space needed: 10-20GB

## 5. Best Practices

### When to Use Column Families (Cassandra)
1. Real-time applications (sub-millisecond responses)
2. High write throughput needed
3. Data naturally groups into families
4. Frequent updates/deletes
5. Row-level consistency important

### When to Use Parquet
1. Large-scale analytics
2. Data warehouse/lake storage
3. Immutable data
4. Complex queries on specific columns
5. Storage efficiency critical

## 6. Hybrid Approaches

Many modern systems use both:
1. Cassandra for real-time OLTP
2. Periodic exports to Parquet for analytics
3. Use tools like Spark to query both simultaneously

Example architecture:
```plaintext
Real-time Data → Cassandra → Spark Job → Parquet Files
                     ↓            ↓           ↓
                Real-time    Interactive   Long-term
                 Queries      Analytics    Analytics
```

This provides:
- Fast real-time access via Cassandra
- Efficient analytics via Parquet
- Cost-effective storage for historical data
