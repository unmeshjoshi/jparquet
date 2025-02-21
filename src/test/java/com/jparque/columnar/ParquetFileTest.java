package com.jparque.columnar;

import com.jparque.columnar.file.ParquetFileReader;
import com.jparque.columnar.file.ParquetFileWriter;
import com.jparque.common.schema.MessageType;
import com.jparque.common.schema.OriginalType;
import com.jparque.common.schema.Repetition;
import com.jparque.common.schema.Type;
import com.jparque.common.schema.*;
import com.jparque.columnar.chunk.CompressionCodec;
import com.jparque.columnar.SerializerConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ParquetFileTest {
    @TempDir
    File tempDir;

    @Test
    void shouldWriteAndReadFile() throws Exception {
        // Create schema
        MessageType schema = new MessageType.Builder("Person")
            .addField("name", Type.BINARY, Repetition.REQUIRED, OriginalType.UTF8)
            .addField("age", Type.INT32, Repetition.REQUIRED)
            .addField("emails", Type.BINARY, Repetition.REPEATED, OriginalType.UTF8)
            .build();

        // Create test data
        List<Map<String, Object>> records = new ArrayList<>();
        Map<String, Object> record1 = new HashMap<>();
        record1.put("name", "Alice");
        record1.put("age", 30);
        record1.put("emails", Arrays.asList("alice@example.com", "alice.work@example.com"));
        records.add(record1);

        Map<String, Object> record2 = new HashMap<>();
        record2.put("name", "Bob");
        record2.put("age", 25);
        record2.put("emails", Arrays.asList("bob@example.com"));
        records.add(record2);

        // Create file path
        Path filePath = tempDir.toPath().resolve("test.parquet");

        // Write data
        SerializerConfig config = new SerializerConfig.Builder()
            .setCompressionCodec(CompressionCodec.UNCOMPRESSED)
            .build();
        ParquetFileWriter writer = new ParquetFileWriter(schema, config);
        writer.write(records, filePath);

        // Verify file exists and has content
        assertThat(Files.exists(filePath)).isTrue();
        assertThat(Files.size(filePath)).isGreaterThan(0);

        // Read data back
        ParquetFileReader reader = new ParquetFileReader();
        List<Map<String, Object>> readRecords = reader.read(filePath);

        // Verify data matches
        assertThat(readRecords).hasSize(2);
        
        Map<String, Object> readRecord1 = readRecords.get(0);
        assertThat(readRecord1)
            .containsEntry("name", "Alice")
            .containsEntry("age", 30);
        @SuppressWarnings("unchecked")
        List<String> emails1 = (List<String>) readRecord1.get("emails");
        assertThat(emails1)
            .containsExactly("alice@example.com", "alice.work@example.com");

        Map<String, Object> readRecord2 = readRecords.get(1);
        assertThat(readRecord2)
            .containsEntry("name", "Bob")
            .containsEntry("age", 25);
        @SuppressWarnings("unchecked")
        List<String> emails2 = (List<String>) readRecord2.get("emails");
        assertThat(emails2)
            .containsExactly("bob@example.com");
    }

    @Test
    void shouldHandleFileNotFound() {
        // Try to read non-existent file
        Path nonExistentFile = tempDir.toPath().resolve("nonexistent.parquet");
        ParquetFileReader reader = new ParquetFileReader();

        assertThatThrownBy(() -> reader.read(nonExistentFile))
            .isInstanceOf(IOException.class)
            .hasMessageContaining("File not found");
    }

    @Test
    void shouldHandleInvalidParquetFile() throws Exception {
        // Create an invalid file
        Path invalidFile = tempDir.toPath().resolve("invalid.parquet");
        Files.write(invalidFile, "This is not a parquet file".getBytes());

        ParquetFileReader reader = new ParquetFileReader();

        assertThatThrownBy(() -> reader.read(invalidFile))
            .isInstanceOf(IOException.class)
            .hasMessageContaining("Invalid Parquet file");
    }

    @Test
    void shouldHandleWriteErrors() throws Exception {
        MessageType schema = new MessageType.Builder("Person")
            .addField("name", Type.BINARY, Repetition.REQUIRED, OriginalType.UTF8)
            .build();

        // Create a read-only directory
        Path readOnlyDir = tempDir.toPath().resolve("readonly");
        Files.createDirectory(readOnlyDir);
        readOnlyDir.toFile().setWritable(false);

        Path filePath = readOnlyDir.resolve("test.parquet");
        ParquetFileWriter writer = new ParquetFileWriter(schema);

        assertThatThrownBy(() -> writer.write(Collections.emptyList(), filePath))
            .isInstanceOf(IOException.class)
            .hasMessageContaining("Unable to write to file");
    }
}
