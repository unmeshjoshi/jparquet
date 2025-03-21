package com.jparque.table;

import com.jparque.btree.BPlusTree;
import com.jparque.storage.Record;
import com.jparque.storage.StorageEngine;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Table is a wrapper around a B+Tree that provides a row-oriented
 * storage interface. Each row is identified by a primary key and
 * contains a set of column values.
 */
public class Table {
    private final StorageEngine storage;
    private final String name;
    private final Path dbFilePath;
    
    /**
     * Creates a new table with the given name using the default page size.
     * 
     * @param dataDirectory Directory where table data will be stored
     * @param name Name of the table
     * @throws IOException If table creation fails
     */
    public Table(Path dataDirectory, String name) throws IOException {
        this.name = name;
        this.dbFilePath = dataDirectory.resolve(name + ".db");
        this.storage = new BPlusTree(dbFilePath);
    }
    
    /**
     * Creates a new table with the given name and custom page size.
     * 
     * @param dataDirectory Directory where table data will be stored
     * @param name Name of the table
     * @param pageSize Size of each page in bytes
     * @throws IOException If table creation fails
     */
    public Table(Path dataDirectory, String name, int pageSize) throws IOException {
        this.name = name;
        this.dbFilePath = dataDirectory.resolve(name + ".db");
        this.storage = new BPlusTree(dbFilePath, pageSize);
    }
    
    /**
     * Insert a new row into the table.
     * 
     * @param key Primary key for the row
     * @param row Map of column names to values
     * @throws IOException If insertion fails
     */
    public void insert(String key, Map<String, Object> row) throws IOException {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        
        // First check if the key already exists
        Optional<Map<String, Object>> existing = storage.read(keyBytes);
        
        // If it exists, delete it first to ensure proper update
        if (existing.isPresent()) {
            storage.delete(keyBytes);
        }
        
        // Then insert the new value
        storage.write(keyBytes, row);
    }
    
    /**
     * Find a row by its primary key.
     * 
     * @param key Primary key to look up
     * @return Optional containing the row if found, empty otherwise
     * @throws IOException If read operation fails
     */
    public Optional<Map<String, Object>> find(String key) throws IOException {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        return storage.read(keyBytes);
    }
    
    /**
     * Insert multiple rows in a batch operation.
     * 
     * @param rows Map of primary keys to row data
     * @throws IOException If batch insertion fails
     */
    public void batchInsert(Map<String, Map<String, Object>> rows) throws IOException {
        // Handle each row individually to ensure proper updates
        for (Map.Entry<String, Map<String, Object>> entry : rows.entrySet()) {
            insert(entry.getKey(), entry.getValue());
        }
    }
    
    /**
     * Scan rows within a key range.
     * 
     * @param startKey Start of the key range (inclusive)
     * @param endKey End of the key range (exclusive)
     * @param columns Specific columns to retrieve, or null for all columns
     * @return Map of keys to rows that fall within the range
     * @throws IOException If scan operation fails
     */
    public Map<String, Map<String, Object>> scan(String startKey, String endKey, List<String> columns) throws IOException {
        byte[] startKeyBytes = startKey.getBytes(StandardCharsets.UTF_8);
        byte[] endKeyBytes = endKey != null ? endKey.getBytes(StandardCharsets.UTF_8) : null;
        
        List<Record> records = storage.scan(startKeyBytes, endKeyBytes, columns);
        Map<String, Map<String, Object>> result = new HashMap<>();
        
        for (Record record : records) {
            String recordKey = new String(record.getKey(), StandardCharsets.UTF_8);
            result.put(recordKey, record.getValue());
        }
        
        return result;
    }
    
    /**
     * Delete a row by its primary key.
     * 
     * @param key Primary key of the row to delete
     * @throws IOException If deletion fails
     */
    public void delete(String key) throws IOException {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        storage.delete(keyBytes);
    }
    
    /**
     * Close the table and release resources.
     * 
     * @throws IOException If close operation fails
     */
    public void close() throws IOException {
        storage.close();
    }
    
    /**
     * Get the name of the table.
     * 
     * @return Table name
     */
    public String getName() {
        return name;
    }
    
    /**
     * Get the path to the table's database file.
     * 
     * @return Path to the database file
     */
    public Path getDbFilePath() {
        return dbFilePath;
    }
} 