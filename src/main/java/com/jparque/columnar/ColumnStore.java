package com.jparque.columnar;

import com.jparque.columnar.file.ParquetFileReader;
import com.jparque.columnar.file.ParquetFileWriter;
import com.jparque.common.schema.*;
import com.jparque.storage.Record;
import com.jparque.storage.StorageEngine;

import java.io.IOException;
import java.util.Base64;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * Columnar storage implementation using Parquet format.
 * Optimized for OLAP workloads with column-oriented storage,
 * compression, and efficient column scans.
 * 
 * This implementation stores multiple records with different keys in a single Parquet file,
 * which better demonstrates the columnar nature of Parquet format.
 */
public class ColumnStore implements StorageEngine {
    private final Path filePath;
    // This schema field is used during initialization and passed to the ParquetSerializer,
    // keeping it as a field for future schema validation and metadata operations
    private final MessageType schema;
    private final ParquetFileWriter writer;
    private final ParquetFileReader reader;
    
    // In-memory cache of records - in a real implementation this would be more sophisticated
    private final List<Map<String, Object>> recordCache = new ArrayList<>();
    
    // Set of deleted keys (stored as Base64 encoded strings)
    private final Set<String> deletedKeys = new HashSet<>();
    
    // Flag to track if the store is open
    private boolean isOpen = true;
    
    /**
     * Creates a new ColumnStore with default configuration.
     * 
     * @param dataDirectory Directory to store data files
     * @param fileName Name of the Parquet file to use for storage
     * @param schema Schema for the column store
     * @throws IOException If directory creation fails
     */
    public ColumnStore(Path dataDirectory, String fileName, MessageType schema) throws IOException {
        this(dataDirectory, fileName, schema, new SerializerConfig.Builder().build());
    }
    
    /**
     * Creates a new ColumnStore with custom configuration.
     * 
     * @param dataDirectory Directory to store data files
     * @param fileName Name of the Parquet file to use for storage
     * @param schema Schema for the column store
     * @param config Serialization configuration
     * @throws IOException If directory creation fails
     */
    public ColumnStore(Path dataDirectory, String fileName, MessageType schema, SerializerConfig config) throws IOException {
        this.schema = schema;
        this.writer = new ParquetFileWriter(schema, config);
        this.reader = new ParquetFileReader();
        
        // Ensure data directory exists
        Files.createDirectories(dataDirectory);
        
        // Set the file path
        this.filePath = dataDirectory.resolve(fileName + ".parquet");
        
        // Load existing data if file exists
        if (Files.exists(filePath)) {
            try {
                recordCache.addAll(reader.read(filePath));
                System.out.println("Loaded " + recordCache.size() + " records from existing file");
            } catch (IOException e) {
                System.err.println("Error loading existing data: " + e.getMessage());
            }
        }
    }
    
    /**
     * Finds a record with a specific key from the cache or the file.
     * 
     * @param key The key to find
     * @return Optional containing the record if found, empty otherwise
     * @throws IOException If there's an error reading the file
     */
    private Optional<Map<String, Object>> findRecordForKey(byte[] key) throws IOException {
        // Check if key is deleted
        String keyString = Base64.getEncoder().encodeToString(key);
        if (deletedKeys.contains(keyString)) {
            return Optional.empty();
        }
        
        // First, check in-memory cache
        for (Map<String, Object> record : recordCache) {
            Object storedKey = record.get("_key");
            if (storedKey instanceof byte[] && Arrays.equals((byte[]) storedKey, key)) {
                return Optional.of(record);
            }
        }
        
        // If not in cache and file exists, try reading from file
        if (Files.exists(filePath) && recordCache.isEmpty()) {
            try {
                List<Map<String, Object>> records = reader.read(filePath);
                
                // Update cache
                recordCache.clear();
                recordCache.addAll(records);
                
                // Search in the newly loaded records
                for (Map<String, Object> record : records) {
                    Object storedKey = record.get("_key");
                    if (storedKey instanceof byte[] && Arrays.equals((byte[]) storedKey, key)) {
                        return Optional.of(record);
                    }
                }
            } catch (IOException e) {
                System.err.println("Error reading file " + filePath + ": " + e.getMessage());
            }
        }
        
        return Optional.empty();
    }

    @Override
    public void write(byte[] key, Map<String, Object> value) throws IOException {
        checkOpen();
        
        // Debug
        System.out.println("Writing key: " + new String(key, java.nio.charset.StandardCharsets.UTF_8));
        System.out.println("Key hash: " + Arrays.hashCode(key));
        
        // Create a copy of the value map and add the key
        Map<String, Object> recordWithKey = new HashMap<>(value);
        byte[] keyCopy = Arrays.copyOf(key, key.length); // Create a defensive copy of the key
        recordWithKey.put("_key", keyCopy);
        
        // If this key already exists, remove old record
        String keyString = Base64.getEncoder().encodeToString(key);
        if (deletedKeys.contains(keyString)) {
            deletedKeys.remove(keyString);
        } else {
            // Remove existing record with this key if any
            recordCache.removeIf(record -> {
                Object storedKey = record.get("_key");
                return storedKey instanceof byte[] && Arrays.equals((byte[]) storedKey, key);
            });
        }
        
        // Add the new record to the cache
        recordCache.add(recordWithKey);
        
        // Write all records to the Parquet file
        writer.write(recordCache, filePath);
        
        System.out.println("Written to file: " + filePath);
    }

    @Override
    public void writeBatch(List<Record> records) throws IOException {
        checkOpen();
        
        if (records.isEmpty()) {
            return;
        }
        
        // Process each record for batch writing
        for (Record record : records) {
            byte[] key = record.getKey();
            String keyString = Base64.getEncoder().encodeToString(key);
            
            // Remove old version of this record if exists
            if (deletedKeys.contains(keyString)) {
                deletedKeys.remove(keyString);
            } else {
                recordCache.removeIf(r -> {
                    Object storedKey = r.get("_key");
                    return storedKey instanceof byte[] && Arrays.equals((byte[]) storedKey, key);
                });
            }
            
            // Add new record to cache
            Map<String, Object> recordMap = new HashMap<>(record.getValue());
            byte[] keyCopy = Arrays.copyOf(key, key.length);
            recordMap.put("_key", keyCopy);
            recordCache.add(recordMap);
        }
        
        // Write all records to the single Parquet file
        flushToDisk();
    }

    @Override
    public Optional<Map<String, Object>> read(byte[] key) throws IOException {
        checkOpen();
        
        // Debug
        System.out.println("Reading key: " + new String(key, java.nio.charset.StandardCharsets.UTF_8));
        System.out.println("Key hash: " + Arrays.hashCode(key));
        
        String keyString = Base64.getEncoder().encodeToString(key);
        System.out.println("Key as Base64: " + keyString);
        
        // Check if the key is deleted
        if (deletedKeys.contains(keyString)) {
            System.out.println("Key is deleted");
            return Optional.empty();
        }
        
        // Find the record with this key
        Optional<Map<String, Object>> recordOpt = findRecordForKey(key);
        if (!recordOpt.isPresent()) {
            System.out.println("No record found for key");
            return Optional.empty();
        }
        
        // Create a copy without the internal _key field
        Map<String, Object> record = recordOpt.get();
        Map<String, Object> resultRecord = new HashMap<>(record);
        resultRecord.remove("_key");
        
        return Optional.of(resultRecord);
    }

    @Override
    public List<Record> scan(byte[] startKey, byte[] endKey, List<String> columns) throws IOException {
        checkOpen();
        
        List<Record> results = new ArrayList<>();
        
        // Ensure our cache is up to date
        if (Files.exists(filePath) && recordCache.isEmpty()) {
            try {
                recordCache.addAll(reader.read(filePath));
            } catch (IOException e) {
                System.err.println("Error reading parquet file: " + e.getMessage());
            }
        }
        
        // Scan through the record cache
        for (Map<String, Object> record : recordCache) {
            Object storedKeyObj = record.get("_key");
            if (!(storedKeyObj instanceof byte[])) {
                continue;
            }
            
            byte[] storedKey = (byte[]) storedKeyObj;
            String keyString = Base64.getEncoder().encodeToString(storedKey);
            
            // Skip deleted keys
            if (deletedKeys.contains(keyString)) {
                continue;
            }
            
            // Check key range
            if (compareKeys(storedKey, startKey) >= 0 && 
                (endKey == null || compareKeys(storedKey, endKey) < 0)) {
                
                // Create a projection if columns are specified
                Map<String, Object> projection = new HashMap<>();
                if (columns != null && !columns.isEmpty()) {
                    for (String column : columns) {
                        if (record.containsKey(column)) {
                            projection.put(column, record.get(column));
                        }
                    }
                } else {
                    projection = new HashMap<>(record);
                    projection.remove("_key");
                }
                
                results.add(new Record(storedKey, projection));
            }
        }
        
        // Sort results by key for consistent output
        results.sort((r1, r2) -> compareKeys(r1.getKey(), r2.getKey()));
        
        return results;
    }

    @Override
    public void delete(byte[] key) throws IOException {
        checkOpen();
        
        String keyString = Base64.getEncoder().encodeToString(key);
        
        // Mark the key as deleted
        deletedKeys.add(keyString);
        
        // Remove from the cache if present
        boolean removed = recordCache.removeIf(record -> {
            Object storedKey = record.get("_key");
            return storedKey instanceof byte[] && Arrays.equals((byte[]) storedKey, key);
        });
        
        if (removed) {
            // Update the file if we removed something from the cache
            flushToDisk();
        }
    }

    @Override
    public void close() throws IOException {
        if (!isOpen) {
            return;
        }
        
        // Flush any pending changes to disk
        flushToDisk();
        isOpen = false;
    }
    
    /**
     * Compares two byte array keys.
     * 
     * @param key1 First key
     * @param key2 Second key
     * @return Negative if key1 < key2, zero if equal, positive if key1 > key2
     */
    private int compareKeys(byte[] key1, byte[] key2) {
        int minLength = Math.min(key1.length, key2.length);
        for (int i = 0; i < minLength; i++) {
            int b1 = key1[i] & 0xFF;  // Convert to unsigned
            int b2 = key2[i] & 0xFF;  // Convert to unsigned
            if (b1 != b2) {
                return b1 - b2;
            }
        }
        return key1.length - key2.length;
    }
    
    /**
     * Flush all cached records to disk.
     * @throws IOException If an I/O error occurs during flush
     */
    private void flushToDisk() throws IOException {
        if (!recordCache.isEmpty()) {
            // Using the writer that was initialized with our schema
            // No need to explicitly use the schema again as it's embedded in the writer
            writer.write(recordCache, filePath);
            System.out.println("Flushed " + recordCache.size() + " records to " + filePath);
        }
    }
    
    /**
     * Checks if the store is open.
     * 
     * @throws IllegalStateException If the store is closed
     */
    private void checkOpen() {
        if (!isOpen) {
            throw new IllegalStateException("ColumnStore is closed");
        }
    }
    
    /**
     * Gets the schema used by this columnar store.
     * 
     * @return The message type schema
     */
    public MessageType getSchema() {
        return schema;
    }
}
