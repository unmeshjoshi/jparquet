# Parquet File Format Internals

## 1. File Structure

### 1.1 Physical Layout
```plaintext
┌────────────────────────┐
│ 4-byte magic number    │
├────────────────────────┤
│ <Column chunk 1>       │
│   - Data pages        │
│   - Dictionary pages  │
├────────────────────────┤
│ <Column chunk 2>       │
│   - Data pages        │
│   - Dictionary pages  │
├────────────────────────┤
│ ...                    │
├────────────────────────┤
│ File Metadata          │
├────────────────────────┤
│ Footer length (4 bytes)│
├────────────────────────┤
│ 4-byte magic number    │
└────────────────────────┘
```

### 1.2 Row Group Structure
```plaintext
Row Group
├── Column Chunk (column 1)
│   ├── Page Header
│   │   ├── Page type (DICTIONARY, DATA)
│   │   ├── Uncompressed size
│   │   ├── Compressed size
│   │   └── Encoding stats
│   ├── Dictionary Page (optional)
│   │   └── Dictionary entries
│   └── Data Pages
│       ├── Definition levels
│       ├── Repetition levels
│       └── Encoded values
└── Column Chunk (column 2)
    └── ...
```

## 2. Memory Management

### 2.1 Reading Process
1. **File Footer** (~10KB)
   - Always read first
   - Contains schema and row group locations

2. **Row Group Metadata** (~100KB per row group)
   - Read when row group is accessed
   - Contains column chunk locations and statistics

3. **Column Chunks**
   - Read only when column is needed
   - Size = (Row Group Size / Number of Columns)
   - Example: 128MB row group with 10 columns = ~12.8MB per chunk

### 2.2 Memory Budget Example
For a 1GB Parquet file:

```plaintext
Fixed Memory:
- File Footer:          10KB
- Row Group Metadata:   800KB (8 row groups)
- Column Metadata:      100KB

Per-Column Reading:
- Dictionary:           1-5MB
- Read Buffer:         1-10MB
- Processing Buffer:   5-20MB

Total per query:      ~50MB
```

## 3. Encoding and Compression

### 3.1 Encoding Types
1. **PLAIN**
   - Direct value encoding
   - Used for: numbers, dates, strings
   - Size: Same as raw data

2. **DICTIONARY**
   - Maps values to integers
   - Used for: low-cardinality columns
   - Compression: 10x-100x for repeated values

3. **RLE (Run Length Encoding)**
   - Encodes repeated values efficiently
   - Used for: sorted data, sparse columns
   - Example:
     ```
     Raw:  AAAAAABBBCC
     RLE:  (A,6)(B,3)(C,2)
     ```

### 3.2 Compression Performance

Using a 100GB dataset with different column types:

| Column Type | Raw Size | Dictionary | Snappy | Zstd |
|------------|----------|------------|---------|------|
| Integer    | 100GB    | 40GB      | 30GB    | 25GB |
| String     | 100GB    | 20GB      | 45GB    | 35GB |
| Timestamp  | 100GB    | N/A       | 35GB    | 30GB |

Compression Speed (MB/s):
```plaintext
Write Speed:
- Uncompressed:  800 MB/s
- Snappy:        600 MB/s
- Zstd:          400 MB/s

Read Speed:
- Uncompressed:  1000 MB/s
- Snappy:        800 MB/s
- Zstd:          700 MB/s
```

## 4. Query Optimization

### 4.1 Predicate Pushdown
Example query:
```sql
SELECT name, email 
FROM users 
WHERE signup_date > '2024-01-01'
```

Processing steps:
1. Read file footer (~10KB)
2. Check row group metadata
3. Skip row groups with max(signup_date) < '2024-01-01'
4. For remaining groups:
   - Read only name, email columns
   - Apply filter on signup_date column

Performance impact:
```plaintext
Without pushdown:
- Read all columns:     100GB
- Process all rows:     1B rows
- Time:                 300s

With pushdown:
- Read 3 columns:       30GB
- Process filtered:     100M rows
- Time:                 40s
```

### 4.2 Column Pruning
Example query:
```sql
SELECT COUNT(DISTINCT email)
FROM users
```

Memory usage:
```plaintext
Without pruning:
- Load all columns:     100GB
- Memory needed:        10GB

With pruning:
- Load email column:    10GB
- Memory needed:        1GB
```

## 5. Performance Tuning

### 5.1 Row Group Size
Impact of different sizes:

| Size   | Pros | Cons |
|--------|------|------|
| 16MB   | - Fast random access<br>- Low memory usage | - More metadata<br>- Worse compression |
| 128MB  | - Good balance<br>- Standard choice | - Medium memory usage |
| 512MB  | - Best compression<br>- Less metadata | - High memory usage<br>- Slower random access |

### 5.2 Page Size
Optimal page sizes for different scenarios:

| Scenario | Page Size | Reason |
|----------|-----------|---------|
| Time series | 512KB | Regular patterns, good compression |
| Random data | 1MB   | Balance between random access and compression |
| Large strings | 2MB | Fewer page headers, better compression |

### 5.3 Real-world Configuration Example

For a 1TB dataset with 100B rows:

```java
ParquetWriter.Builder<Group> builder = ExampleParquetWriter
    .builder(path)
    .withRowGroupSize(128 * 1024 * 1024)    // 128MB row groups
    .withPageSize(1024 * 1024)              // 1MB pages
    .withDictionaryPageSize(5 * 1024 * 1024) // 5MB dictionary pages
    .withDictionaryEncoding(true)
    .withValidation(true)
    .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0);

// Results in:
// - 8000 row groups
// - ~100K pages per column
// - 2-5x compression ratio
// - 50MB memory per query
```
