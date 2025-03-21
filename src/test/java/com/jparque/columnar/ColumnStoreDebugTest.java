package com.jparque.columnar;

import com.jparque.common.schema.MessageType;
import com.jparque.common.schema.OriginalType;
import com.jparque.common.schema.Repetition;
import com.jparque.common.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ColumnStoreDebugTest {
    @TempDir
    Path tempDir;
    
    private ColumnStore store;
    private MessageType schema;
    
    @BeforeEach
    void setUp() throws IOException {
        // Create a simple schema
        schema = new MessageType.Builder("Simple")
            .addField("value", Type.BINARY, Repetition.REQUIRED, OriginalType.UTF8)
            .build();
        
        // Create a data directory within the temp directory
        Path dataDir = tempDir.resolve("debug_data");
        Files.createDirectories(dataDir);
        
        // Create the store with a default file name
        store = new ColumnStore(dataDir, "debug_store.parquet", schema);
    }
    
    @AfterEach
    void tearDown() throws IOException {
        // Close the store
        if (store != null) {
            store.close();
        }
    }
    
    @Test
    void debugTestWriteAndRead() throws IOException {
        // Print the schema
        System.out.println("============== DEBUG TEST START ==============");
        System.out.println("Schema: " + schema);
        System.out.println("Test Data Directory: " + tempDir);
        
        // Create a simple key and value
        byte[] key = "test-key".getBytes();
        Map<String, Object> value = new HashMap<>();
        value.put("value", "test-value");
        
        // Print key info
        String keyBase64 = Base64.getEncoder().encodeToString(key);
        System.out.println("Writing key: " + new String(key) + " (Base64: " + keyBase64 + ")");
        System.out.println("Key bytes: " + Arrays.toString(key));
        System.out.println("Value: " + value);
        
        // Write to store
        System.out.println("Calling store.write...");
        store.write(key, value);
        System.out.println("Write completed");
        
        // List files in data directory
        System.out.println("Files in data directory after write:");
        Files.list(tempDir.resolve("debug_data")).forEach(p -> {
            System.out.println(" - " + p);
            try {
                System.out.println("   Size: " + Files.size(p) + " bytes");
            } catch (IOException e) {
                System.out.println("   Error getting size: " + e);
            }
        });
        
        // Verify keyToFileMap content by reflection
        try {
            java.lang.reflect.Field field = ColumnStore.class.getDeclaredField("keyToFileMap");
            field.setAccessible(true);
            Map<?, ?> map = (Map<?, ?>) field.get(store);
            System.out.println("Internal keyToFileMap contents:");
            map.forEach((k, v) -> System.out.println(" - " + k + " -> " + v));
        } catch (Exception e) {
            System.out.println("Error accessing keyToFileMap: " + e);
        }
        
        // Try to read the key
        System.out.println("Calling store.read with same key object...");
        Optional<Map<String, Object>> result = store.read(key);
        
        // Check the result
        System.out.println("Read result present: " + result.isPresent());
        if (result.isPresent()) {
            System.out.println("Read value: " + result.get());
        } else {
            System.out.println("Read FAILED - value not found");
        }
        
        // Create a new key with same content to test equality
        byte[] newKey = "test-key".getBytes();
        System.out.println("Trying with new key object with same content:");
        System.out.println("Original key: " + Arrays.toString(key));
        System.out.println("New key: " + Arrays.toString(newKey));
        System.out.println("Keys equal by Arrays.equals: " + Arrays.equals(key, newKey));
        System.out.println("Keys equal by ==: " + (key == newKey));
        
        // Try with new key object
        result = store.read(newKey);
        System.out.println("Read with new key object result present: " + result.isPresent());
        
        // Assertions
        assertTrue(result.isPresent(), "Record should be present");
        assertEquals("test-value", result.get().get("value"), "Value should match");
        System.out.println("============== DEBUG TEST END ==============");
    }
}
