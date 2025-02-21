package com.jparque.columnar.file;

import com.jparque.columnar.ParquetDeserializer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * High-level reader for Parquet files.
 * Provides a simplified interface for reading data from Parquet files.
 */
public class ParquetFileReader {
    private final ParquetDeserializer deserializer;

    /**
     * Creates a new ParquetFileReader.
     */
    public ParquetFileReader() {
        this.deserializer = new ParquetDeserializer();
    }

    /**
     * Reads all records from a Parquet file at the specified path.
     *
     * @param path The path to read from
     * @return A list of records, where each record is a map of field names to values
     * @throws IOException if there's an error reading the file
     */
    public List<Map<String, Object>> read(Path path) throws IOException {
        if (!Files.exists(path)) {
            throw new IOException("File not found: " + path);
        }

        try {
            return deserializer.deserialize(path.toString());
        } catch (IOException e) {
            if (e.getMessage() != null && e.getMessage().contains("incorrect magic number")) {
                throw new IOException("Invalid Parquet file: " + path, e);
            }
            throw new IOException("Error reading file: " + path, e);
        }
    }
}
