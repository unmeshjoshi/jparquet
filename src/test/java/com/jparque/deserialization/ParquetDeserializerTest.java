package com.jparque.deserialization;

import com.jparque.columnar.ParquetDeserializer;
import com.jparque.common.schema.*;
import com.jparque.columnar.ParquetSerializer;
import com.jparque.columnar.SerializerConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ParquetDeserializerTest {
    @TempDir
    File tempDir;

    @Test
    void shouldDeserializeMultipleRecords() throws Exception {
        System.out.println("Starting test: shouldDeserializeMultipleRecords");
        // Create a simple schema
        MessageType schema = new MessageType.Builder("Person")
            .addField("name", Type.BINARY, Repetition.REQUIRED, OriginalType.UTF8)
            .addField("age", Type.INT32, Repetition.REQUIRED)
            .build();

        // Create test data
        Map<String, Object> person1 = new HashMap<>();
        person1.put("name", "Alice");
        person1.put("age", 30);

        Map<String, Object> person2 = new HashMap<>();
        person2.put("name", "Bob");
        person2.put("age", 25);

        List<Map<String, Object>> records = Arrays.asList(person1, person2);

        // Write test data to a Parquet file
        File parquetFile = new File(tempDir, "test.parquet");
        System.out.println("Writing to file: " + parquetFile.getAbsolutePath());
        ParquetSerializer serializer = new ParquetSerializer(schema);
        serializer.serialize(records, parquetFile.getAbsolutePath());
        System.out.println("File exists: " + parquetFile.exists() + ", size: " + parquetFile.length());

        // Read data back using deserializer
        ParquetDeserializer deserializer = new ParquetDeserializer();
        List<Map<String, Object>> deserializedRecords = deserializer.deserialize(parquetFile.getAbsolutePath());

        // Verify deserialized data matches original
        assertThat(deserializedRecords).hasSize(2);

        Map<String, Object> deserializedPerson1 = deserializedRecords.get(0);
        assertThat(deserializedPerson1)
            .containsEntry("name", "Alice")
            .containsEntry("age", 30);

        Map<String, Object> deserializedPerson2 = deserializedRecords.get(1);
        assertThat(deserializedPerson2)
            .containsEntry("name", "Bob")
            .containsEntry("age", 25);
    }

    @Test
    void shouldDeserializeNullableFields() throws Exception {
        // Create schema with nullable fields
        MessageType schema = new MessageType.Builder("Person")
            .addField("name", Type.BINARY, Repetition.REQUIRED, OriginalType.UTF8)
            .addField("age", Type.INT32, Repetition.OPTIONAL)
            .addField("email", Type.BINARY, Repetition.OPTIONAL, OriginalType.UTF8)
            .build();

        // Create test data with null fields
        Map<String, Object> person = new HashMap<>();
        person.put("name", "Alice");
        person.put("age", null);
        // Don't set email field

        List<Map<String, Object>> records = Arrays.asList(person);

        // Write test data
        File parquetFile = new File(tempDir, "nullable.parquet");
        ParquetSerializer serializer = new ParquetSerializer(schema);
        serializer.serialize(records, parquetFile.getAbsolutePath());

        // Read data back
        ParquetDeserializer deserializer = new ParquetDeserializer();
        List<Map<String, Object>> deserializedRecords = deserializer.deserialize(parquetFile.getAbsolutePath());

        // Verify deserialized data
        assertThat(deserializedRecords).hasSize(1);
        Map<String, Object> deserializedPerson = deserializedRecords.get(0);
        assertThat(deserializedPerson)
            .containsEntry("name", "Alice")
            .containsEntry("age", null)
            .containsEntry("email", null);
    }

    @Test
    void shouldDeserializeRepeatedFields() throws Exception {
        // Create schema with repeated field
        MessageType schema = new MessageType.Builder("Person")
            .addField("name", Type.BINARY, Repetition.REQUIRED, OriginalType.UTF8)
            .addField("tags", Type.BINARY, Repetition.REPEATED, OriginalType.UTF8)
            .build();

        // Create test data with repeated field
        Map<String, Object> person = new HashMap<>();
        person.put("name", "Alice");
        person.put("tags", Arrays.asList("developer", "java"));

        List<Map<String, Object>> records = Arrays.asList(person);

        // Write test data
        File parquetFile = new File(tempDir, "repeated.parquet");
        ParquetSerializer serializer = new ParquetSerializer(schema);
        serializer.serialize(records, parquetFile.getAbsolutePath());

        // Read data back
        ParquetDeserializer deserializer = new ParquetDeserializer();
        List<Map<String, Object>> deserializedRecords = deserializer.deserialize(parquetFile.getAbsolutePath());

        // Verify deserialized data
        assertThat(deserializedRecords).hasSize(1);
        Map<String, Object> deserializedPerson = deserializedRecords.get(0);
        assertThat(deserializedPerson)
            .containsEntry("name", "Alice")
            .hasEntrySatisfying("tags", value -> {
                assertThat(value).isInstanceOf(List.class);
                @SuppressWarnings("unchecked")
                List<String> tags = (List<String>) value;
                assertThat(tags).containsExactly("developer", "java");
            });
    }
}
