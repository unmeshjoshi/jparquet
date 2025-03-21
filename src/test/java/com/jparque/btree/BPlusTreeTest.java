package com.jparque.btree;

import com.jparque.storage.Record;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

public class BPlusTreeTest {
    
    @TempDir
    Path tempDir;
    
    private Path dbFile;
    private BPlusTree tree;
    
    @BeforeEach
    public void setUp() throws IOException {
        dbFile = tempDir.resolve("test.db");
        tree = new BPlusTree(dbFile);
    }
    
    @AfterEach
    public void tearDown() throws IOException {
        if (tree != null) {
            tree.close();
        }
    }
    
    @Test
    public void shouldInsertAndReadSingleRecord() throws IOException {
        // Create a test record
        byte[] key = "test-key".getBytes();
        Map<String, Object> value = new HashMap<>();
        value.put("name", "John Doe");
        value.put("age", 30);
        value.put("email", "john@example.com");
        
        // Write to the tree
        tree.write(key, value);
        
        // Read it back
        Optional<Map<String, Object>> result = tree.read(key);
        
        // Debug output
        System.out.println("Test key: " + new String(key));
        System.out.println("Result present: " + result.isPresent());
        if (result.isPresent()) {
            System.out.println("Result contents: " + result.get());
            System.out.println("'name' value: " + result.get().get("name"));
            System.out.println("'age' value: " + result.get().get("age"));
            System.out.println("'email' value: " + result.get().get("email"));
        }
        
        // Verify
        assertTrue(result.isPresent(), "Result should be present");
        assertEquals("John Doe", result.get().get("name"), "Name should match");
        assertEquals(30, result.get().get("age"), "Age should match");
        assertEquals("john@example.com", result.get().get("email"), "Email should match");
    }
    
    @Test
    public void shouldHandleMultipleRecords() throws IOException {
        // Insert multiple records
        for (int i = 0; i < 100; i++) {
            byte[] key = ("key-" + i).getBytes();
            Map<String, Object> value = new HashMap<>();
            value.put("index", i);
            value.put("data", "Value for " + i);
            
            System.out.println("Writing key-" + i);
            tree.write(key, value);
            
            // Validate immediately after writing to isolate issues
            if (i >= 50 && i < 55) { // Just check a few early ones
                byte[] readKey = ("key-" + i).getBytes();
                Optional<Map<String, Object>> readResult = tree.read(readKey);
                System.out.println("Immediate read check for key-" + i + ": " + readResult.isPresent());
                if (readResult.isPresent()) {
                    System.out.println("  Value: " + readResult.get());
                }
            }
        }
        
        // Read them back in random order
        for (int i = 50; i < 75; i++) {
            byte[] key = ("key-" + i).getBytes();
            System.out.println("Reading key-" + i);
            Optional<Map<String, Object>> result = tree.read(key);
            
            System.out.println("Result present: " + result.isPresent());
            if (result.isPresent()) {
                System.out.println("  Value: " + result.get());
            }
            
            assertTrue(result.isPresent(), "Result for key-" + i + " should be present");
            if (result.isPresent()) {
                assertEquals(i, result.get().get("index"), "Index value should match for key-" + i);
                assertEquals("Value for " + i, result.get().get("data"), "Data value should match for key-" + i);
            }
        }
    }
    
    @Test
    public void shouldPerformRangeScan() throws IOException {
        // Insert records with sorted keys
        for (int i = 10; i < 50; i++) {
            byte[] key = String.format("key-%03d", i).getBytes(); // Ensure proper sorting
            Map<String, Object> value = new HashMap<>();
            value.put("index", i);
            
            tree.write(key, value);
        }
        
        // Perform range scan
        byte[] startKey = "key-020".getBytes();
        byte[] endKey = "key-030".getBytes();
        List<Record> results = tree.scan(startKey, endKey, null);
        
        // Verify results
        assertEquals(11, results.size()); // Should include 020 through 030
        
        // Check first and last
        assertEquals("key-020", new String(results.get(0).getKey()));
        assertEquals(20, results.get(0).getValue().get("index"));
        
        assertEquals("key-030", new String(results.get(10).getKey()));
        assertEquals(30, results.get(10).getValue().get("index"));
    }
    
    @Test
    public void shouldProjectColumns() throws IOException {
        // Create a record with multiple fields
        byte[] key = "projection-test".getBytes();
        Map<String, Object> value = new HashMap<>();
        value.put("field1", "value1");
        value.put("field2", "value2");
        value.put("field3", 123);
        value.put("field4", true);
        
        tree.write(key, value);
        
        // Scan with column projection
        List<String> columns = Arrays.asList("field1", "field3");
        List<Record> results = tree.scan(key, "projection-test\0".getBytes(), columns);
        
        // Verify
        assertEquals(1, results.size());
        Map<String, Object> projectedValue = results.get(0).getValue();
        
        assertEquals(2, projectedValue.size());
        assertEquals("value1", projectedValue.get("field1"));
        assertEquals(123, projectedValue.get("field3"));
        assertNull(projectedValue.get("field2"));
        assertNull(projectedValue.get("field4"));
    }
    
    @Test
    public void shouldDeleteRecord() throws IOException {
        // Create and insert a record
        byte[] key = "delete-test".getBytes();
        Map<String, Object> value = new HashMap<>();
        value.put("test", "value");
        
        tree.write(key, value);
        
        // Verify it exists
        assertTrue(tree.read(key).isPresent());
        
        // Delete it
        tree.delete(key);
        
        // Verify it's gone
        assertFalse(tree.read(key).isPresent());
    }
    
    @Test
    public void shouldHandleTreeSplits() throws IOException {
        // Insert enough records to cause multiple tree splits
        // This tests the B+Tree balancing logic
        for (int i = 0; i < 1000; i++) {
            byte[] key = String.format("split-test-%04d", i).getBytes();
            Map<String, Object> value = new HashMap<>();
            value.put("index", i);
            
            tree.write(key, value);
        }
        
        // Verify random samples
        for (int i = 0; i < 1000; i += 50) {
            byte[] key = String.format("split-test-%04d", i).getBytes();
            Optional<Map<String, Object>> result = tree.read(key);
            
            assertTrue(result.isPresent());
            assertEquals(i, result.get().get("index"));
        }
    }
    
    @Test
    public void shouldHandleLargeValues() throws IOException {
        // Create a test record with a large value
        byte[] key = "large-value".getBytes();
        Map<String, Object> value = new HashMap<>();
        
        // Create a large string (> 100KB)
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 20000; i++) {
            sb.append("This is a large value that should be stored in overflow pages. ");
        }
        String largeString = sb.toString();
        value.put("largeText", largeString);
        
        // Write to the tree
        tree.write(key, value);
        
        // Read it back
        Optional<Map<String, Object>> result = tree.read(key);
        
        // Verify
        assertTrue(result.isPresent());
        assertEquals(largeString, result.get().get("largeText"));
    }
    
    @Test
    public void shouldUpdateLargeValue() throws IOException {
        // Create initial large value
        byte[] key = "update-large".getBytes();
        Map<String, Object> value = new HashMap<>();
        
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 10000; i++) {
            sb.append("Initial large value. ");
        }
        String initialValue = sb.toString();
        value.put("text", initialValue);
        
        // Write to the tree
        tree.write(key, value);
        
        // Create a different large value
        Map<String, Object> updatedValue = new HashMap<>();
        sb = new StringBuilder();
        for (int i = 0; i < 15000; i++) {
            sb.append("Updated large value! ");
        }
        String newValue = sb.toString();
        updatedValue.put("text", newValue);
        
        // Update the record
        tree.write(key, updatedValue);
        
        // Read it back
        Optional<Map<String, Object>> result = tree.read(key);
        
        // Verify it was updated
        assertTrue(result.isPresent());
        assertEquals(newValue, result.get().get("text"));
        assertNotEquals(initialValue, result.get().get("text"));
    }
    
    @Test
    public void shouldDeleteLargeValue() throws IOException {
        // Create a test record with a large value
        byte[] key = "delete-large".getBytes();
        Map<String, Object> value = new HashMap<>();
        
        // Create a large string (> 100KB)
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 20000; i++) {
            sb.append("Large value to be deleted. ");
        }
        value.put("largeText", sb.toString());
        
        // Write to the tree
        tree.write(key, value);
        
        // Verify it exists
        assertTrue(tree.read(key).isPresent());
        
        // Delete it
        tree.delete(key);
        
        // Verify it's gone
        assertFalse(tree.read(key).isPresent());
    }
}
