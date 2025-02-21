package com.jparque.columnar;

import com.jparque.common.schema.*;
import com.jparque.columnar.chunk.CompressionCodec;
import com.jparque.columnar.ParquetSerializer;
import com.jparque.columnar.SerializerConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ParquetSerializerTest {
    @TempDir
    File tempDir;

    @Test
    void shouldSerializeDataAccordingToSchema() throws IOException {
        // Create a schema for a person with name, age, and tags
        MessageType schema = new MessageType.Builder("Person")
            .addField("name", Type.BINARY, Repetition.REQUIRED, OriginalType.UTF8)
            .addField("age", Type.INT32, Repetition.REQUIRED)
            .addField("tags", Type.BINARY, Repetition.REPEATED, OriginalType.UTF8)
            .build();

        // Create test data
        Map<String, Object> person1 = new HashMap<>();
        person1.put("name", "Alice");
        person1.put("age", 30);
        person1.put("tags", Arrays.asList("developer", "java"));

        Map<String, Object> person2 = new HashMap<>();
        person2.put("name", "Bob");
        person2.put("age", 25);
        person2.put("tags", Arrays.asList("designer", "ui"));

        List<Map<String, Object>> records = Arrays.asList(person1, person2);

        // Create output file
        File outputFile = new File(tempDir, "test.parquet");
        ParquetSerializer serializer = new ParquetSerializer(schema);
        
        // Write the data
        serializer.serialize(records, outputFile.getAbsolutePath());

        // Verify the file exists and has non-zero size
        assertThat(outputFile).exists();
        assertThat(outputFile.length()).isGreaterThan(0);

        // Verify the Parquet file structure
        try (RandomAccessFile raf = new RandomAccessFile(outputFile, "r")) {
            // Read magic number from start of file
            byte[] magic = new byte[4];
            raf.read(magic);
            assertThat(magic).isEqualTo(new byte[] {'P', 'A', 'R', '1'});

            // Read magic number from end of file
            raf.seek(raf.length() - 4);
            raf.read(magic);
            assertThat(magic).isEqualTo(new byte[] {'P', 'A', 'R', '1'});

            // TODO: Once we implement the full format, we'll add more structural validations here
            // - Verify file metadata
            // - Verify row group structure
            // - Verify column chunks
            // - Verify page headers
        }
    }

    @Test
    void shouldValidateDataAgainstSchema() {
        MessageType schema = new MessageType.Builder("Person")
            .addField("name", Type.BINARY, Repetition.REQUIRED, OriginalType.UTF8)
            .addField("age", Type.INT32, Repetition.REQUIRED)
            .build();

        Map<String, Object> invalidPerson = new HashMap<>();
        invalidPerson.put("name", "Alice");
        // Missing required 'age' field

        ParquetSerializer serializer = new ParquetSerializer(schema);

        assertThatThrownBy(() -> 
            serializer.serialize(Arrays.asList(invalidPerson), "test.parquet")
        ).isInstanceOf(IllegalArgumentException.class)
         .hasMessageContaining("Missing required field: age");
    }

    @ParameterizedTest
    @EnumSource(value = CompressionCodec.class, names = {"UNCOMPRESSED", "SNAPPY", "GZIP", "ZSTD"})
    void shouldCompressData(CompressionCodec codec) throws IOException {
        // Create a schema with a large repeatable field to test compression
        MessageType schema = new MessageType.Builder("Data")
            .addField("content", Type.BINARY, Repetition.REQUIRED, OriginalType.UTF8)
            .build();

        // Create test data with repeating content to make it compressible
        StringBuilder largeContent = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            largeContent.append("test test test test ");
        }

        Map<String, Object> record = new HashMap<>();
        record.put("content", largeContent.toString());

        // Create serializer with compression
        SerializerConfig config = new SerializerConfig.Builder()
            .setCompressionCodec(codec)
            .build();
        ParquetSerializer serializer = new ParquetSerializer(schema, config);

        // Write the data
        File outputFile = new File(tempDir, "compressed.parquet");
        serializer.serialize(Arrays.asList(record), outputFile.getAbsolutePath());

        // For non-UNCOMPRESSED codecs, verify file is actually compressed
        if (codec != CompressionCodec.UNCOMPRESSED) {
            // Write uncompressed version for comparison
            SerializerConfig uncompressedConfig = new SerializerConfig.Builder()
                .setCompressionCodec(CompressionCodec.UNCOMPRESSED)
                .build();
            ParquetSerializer uncompressedSerializer = new ParquetSerializer(schema, uncompressedConfig);
            File uncompressedFile = new File(tempDir, "uncompressed.parquet");
            uncompressedSerializer.serialize(Arrays.asList(record), uncompressedFile.getAbsolutePath());

            // Compare sizes
            assertThat(outputFile.length()).isLessThan(uncompressedFile.length());
        }
    }

    @Test
    void shouldValidateDataTypes() {
        MessageType schema = new MessageType.Builder("Person")
            .addField("name", Type.BINARY, Repetition.REQUIRED, OriginalType.UTF8)
            .addField("age", Type.INT32, Repetition.REQUIRED)
            .build();

        Map<String, Object> invalidPerson = new HashMap<>();
        invalidPerson.put("name", "Alice");
        invalidPerson.put("age", "thirty"); // Wrong type, should be integer

        ParquetSerializer serializer = new ParquetSerializer(schema);

        assertThatThrownBy(() -> 
            serializer.serialize(Arrays.asList(invalidPerson), "test.parquet")
        ).isInstanceOf(IllegalArgumentException.class)
         .hasMessageContaining("Invalid type for field 'age'");
    }

    @Test
    void shouldHandleRepeatedFields() throws IOException {
        MessageType schema = new MessageType.Builder("Person")
            .addField("name", Type.BINARY, Repetition.REQUIRED, OriginalType.UTF8)
            .addField("tags", Type.BINARY, Repetition.REPEATED, OriginalType.UTF8)
            .build();

        Map<String, Object> person = new HashMap<>();
        person.put("name", "Alice");
        person.put("tags", Arrays.asList("developer", "java", "parquet"));

        ParquetSerializer serializer = new ParquetSerializer(schema);

        // Should not throw any exception
        File outputFile = new File(tempDir, "test.parquet");
        serializer.serialize(Arrays.asList(person), outputFile.getAbsolutePath());
        
        // Verify the file was created
        assertThat(outputFile).exists();
        assertThat(outputFile.length()).isGreaterThan(0);
    }
}
