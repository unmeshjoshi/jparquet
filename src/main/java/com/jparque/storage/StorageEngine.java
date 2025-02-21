package com.jparque.storage;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Core interface for all storage engine implementations.
 */
public interface StorageEngine {
    /**
     * Writes a record to the storage engine.
     *
     * @param key The record key
     * @param value The record value as a map of field names to values
     * @throws IOException If an I/O error occurs
     */
    void write(byte[] key, Map<String, Object> value) throws IOException;

    /**
     * Writes multiple records in batch.
     *
     * @param records List of records to write
     * @throws IOException If an I/O error occurs
     */
    void writeBatch(List<Record> records) throws IOException;

    /**
     * Reads a record by its key.
     *
     * @param key The record key
     * @return Optional containing the record if found, empty otherwise
     * @throws IOException If an I/O error occurs
     */
    Optional<Map<String, Object>> read(byte[] key) throws IOException;

    /**
     * Scans records within a key range.
     *
     * @param startKey Start of the key range (inclusive)
     * @param endKey End of the key range (exclusive)
     * @param columns Specific columns to read, or null for all columns
     * @return List of matching records
     * @throws IOException If an I/O error occurs
     */
    List<Record> scan(byte[] startKey, byte[] endKey, List<String> columns) throws IOException;

    /**
     * Deletes a record by its key.
     *
     * @param key The record key
     * @throws IOException If an I/O error occurs
     */
    void delete(byte[] key) throws IOException;

    /**
     * Closes the storage engine and releases resources.
     *
     * @throws IOException If an I/O error occurs
     */
    void close() throws IOException;
}
