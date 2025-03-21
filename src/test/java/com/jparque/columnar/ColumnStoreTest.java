package com.jparque.columnar;

import com.jparque.common.schema.MessageType;
import com.jparque.common.schema.OriginalType;
import com.jparque.common.schema.Repetition;
import com.jparque.common.schema.Type;
import com.jparque.storage.Record;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class ColumnStoreTest {
    @TempDir
    Path tempDir;
    
    private ColumnStore store;
    private MessageType schema;
    
    @BeforeEach
    void setUp() throws IOException {
        // Create a schema for customer data
        schema = new MessageType.Builder("Customer")
            .addField("name", Type.BINARY, Repetition.REQUIRED, OriginalType.UTF8)
            .addField("age", Type.INT32, Repetition.REQUIRED)
            .addField("email", Type.BINARY, Repetition.OPTIONAL, OriginalType.UTF8)
            .addField("tags", Type.BINARY, Repetition.REPEATED, OriginalType.UTF8)
            .build();
        
        // Create a data directory within the temp directory
        Path dataDir = tempDir.resolve("data");
        Files.createDirectories(dataDir);
        
        // Create the store with a default file name
        store = new ColumnStore(dataDir, "store.parquet", schema);
    }
    
    @AfterEach
    void tearDown() throws IOException {
        // Close the store
        if (store != null) {
            store.close();
        }
    }
    
    @Test
    void shouldWriteAndReadSingleRecord() throws IOException {
        // Arrange
        byte[] key = "customer:1".getBytes();
        Map<String, Object> value = new HashMap<>();
        value.put("name", "Alice");
        value.put("age", 30);
        value.put("email", "alice@example.com");
        value.put("tags", Arrays.asList("premium", "loyal"));
        
        // Act
        store.write(key, value);
        Optional<Map<String, Object>> result = store.read(key);
        
        // Assert
        assertThat(result).isPresent();
        Map<String, Object> readValue = result.get();
        assertThat(readValue)
            .containsEntry("name", "Alice")
            .containsEntry("age", 30)
            .containsEntry("email", "alice@example.com");
        
        @SuppressWarnings("unchecked")
        List<String> tags = (List<String>) readValue.get("tags");
        assertThat(tags).containsExactly("premium", "loyal");
    }
    
    @Test
    void shouldHandleNonExistentKey() throws IOException {
        // Arrange
        byte[] key = "customer:nonexistent".getBytes();
        
        // Act
        Optional<Map<String, Object>> result = store.read(key);
        
        // Assert
        assertThat(result).isEmpty();
    }
    
    @Test
    void shouldWriteAndDeleteRecord() throws IOException {
        // Arrange
        byte[] key = "customer:2".getBytes();
        Map<String, Object> value = new HashMap<>();
        value.put("name", "Bob");
        value.put("age", 25);
        
        // Act - Write and read to confirm it exists
        store.write(key, value);
        Optional<Map<String, Object>> resultBeforeDelete = store.read(key);
        
        // Delete the record
        store.delete(key);
        Optional<Map<String, Object>> resultAfterDelete = store.read(key);
        
        // Assert
        assertThat(resultBeforeDelete).isPresent();
        assertThat(resultAfterDelete).isEmpty();
    }
    
    @Test
    void shouldWriteBatchOfRecords() throws IOException {
        // Arrange
        List<Record> records = new ArrayList<>();
        
        // Create 5 customer records
        for (int i = 1; i <= 5; i++) {
            byte[] key = ("customer:" + i).getBytes();
            Map<String, Object> value = new HashMap<>();
            value.put("name", "Customer " + i);
            value.put("age", 20 + i);
            records.add(new Record(key, value));
        }
        
        // Act
        store.writeBatch(records);
        
        // Assert - Read each record and verify
        for (Record record : records) {
            Optional<Map<String, Object>> result = store.read(record.getKey());
            assertThat(result).isPresent();
            Map<String, Object> readValue = result.get();
            assertThat(readValue).containsAllEntriesOf(record.getValue());
        }
    }
    
    @Test
    void shouldScanRecordsInRange() throws IOException {
        // Arrange - Write records with ordered keys
        List<Record> records = new ArrayList<>();
        for (int i = 10; i < 20; i++) {
            byte[] key = String.format("key:%02d", i).getBytes();
            Map<String, Object> value = new HashMap<>();
            value.put("name", "Record " + i);
            value.put("age", i);
            records.add(new Record(key, value));
        }
        store.writeBatch(records);
        
        // Act - Scan a range of keys
        byte[] startKey = "key:12".getBytes();
        byte[] endKey = "key:16".getBytes();
        List<Record> results = store.scan(startKey, endKey, null);
        
        // Assert
        assertThat(results).hasSize(4); // keys 12, 13, 14, 15 (endKey is exclusive)
        
        // Verify the keys are in the expected range and sorted
        List<String> resultKeys = results.stream()
            .map(r -> new String(r.getKey()))
            .collect(Collectors.toList());
        
        assertThat(resultKeys).containsExactly("key:12", "key:13", "key:14", "key:15");
    }
    
    @Test
    void shouldScanWithColumnProjection() throws IOException {
        // Arrange - Write records with multiple columns
        byte[] key = "customer:projection".getBytes();
        Map<String, Object> value = new HashMap<>();
        value.put("name", "Charlie");
        value.put("age", 40);
        value.put("email", "charlie@example.com");
        value.put("tags", Arrays.asList("vip", "new"));
        store.write(key, value);
        
        // Act - Scan with specific columns
        List<String> columns = Arrays.asList("name", "email");
        List<Record> results = store.scan(key, null, columns);
        
        // Assert
        assertThat(results).hasSize(1);
        Map<String, Object> projection = results.get(0).getValue();
        
        // Should only contain the projected columns
        assertThat(projection)
            .containsEntry("name", "Charlie")
            .containsEntry("email", "charlie@example.com")
            .doesNotContainKey("age")
            .doesNotContainKey("tags");
    }
    
    @Test
    void shouldHandleEmptyBatchWrite() throws IOException {
        // Act - Write an empty batch
        store.writeBatch(Collections.emptyList());
        
        // This test passes if no exception is thrown
    }
    
    @Test
    void shouldHandleRangeWithNoResults() throws IOException {
        // Arrange - Write some records
        byte[] key1 = "rangetest:1".getBytes();
        byte[] key2 = "rangetest:5".getBytes();
        
        Map<String, Object> value = new HashMap<>();
        value.put("name", "Range Test");
        value.put("age", 50);
        
        store.write(key1, value);
        store.write(key2, value);
        
        // Act - Scan a range with no records
        byte[] startKey = "rangetest:2".getBytes();
        byte[] endKey = "rangetest:4".getBytes();
        List<Record> results = store.scan(startKey, endKey, null);
        
        // Assert
        assertThat(results).isEmpty();
    }
}
