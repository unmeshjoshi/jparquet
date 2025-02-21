package com.jparque.columnar.file;

import com.jparque.common.schema.MessageType;
import com.jparque.columnar.ParquetSerializer;
import com.jparque.columnar.SerializerConfig;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * High-level writer for Parquet files.
 * Provides a simplified interface for writing data to Parquet files.
 */
public class ParquetFileWriter {
    private final MessageType schema;
    private final SerializerConfig config;

    /**
     * Creates a new ParquetFileWriter with the given schema and default configuration.
     *
     * @param schema The schema to use for writing data
     */
    public ParquetFileWriter(MessageType schema) {
        this(schema, new SerializerConfig.Builder().build());
    }

    /**
     * Creates a new ParquetFileWriter with the given schema and configuration.
     *
     * @param schema The schema to use for writing data
     * @param config The configuration to use for writing data
     */
    public ParquetFileWriter(MessageType schema, SerializerConfig config) {
        this.schema = schema;
        this.config = config;
    }

    /**
     * Writes the given records to a Parquet file at the specified path.
     *
     * @param records The records to write
     * @param path The path to write to
     * @throws IOException if there's an error writing the file
     */
    public void write(List<Map<String, Object>> records, Path path) throws IOException {
        try {
            ParquetSerializer serializer = new ParquetSerializer(schema, config);
            serializer.serialize(records, path.toString());
        } catch (IOException e) {
            throw new IOException("Unable to write to file: " + path, e);
        }
    }
}
