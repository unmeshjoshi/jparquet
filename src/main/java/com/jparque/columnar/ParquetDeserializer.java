package com.jparque.columnar;

import com.jparque.common.schema.*;
import com.jparque.columnar.chunk.CompressionCodec;
import com.jparque.common.compression.Compressor;
import com.jparque.common.compression.CompressorFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Deserializes data from Parquet format into Java objects.
 */
public class ParquetDeserializer {
    private static final byte[] PARQUET_MAGIC = new byte[]{'P', 'A', 'R', '1'};
    private CompressionCodec compressionCodec = CompressionCodec.UNCOMPRESSED;

    /**
     * Deserializes data from a Parquet file into a list of records.
     *
     * @param filePath Path to the Parquet file to read
     * @return List of records as Map&lt;String, Object&gt;
     * @throws IOException if there's an error reading from the file
     */
    public List<Map<String, Object>> deserialize(String filePath) throws IOException {
        System.out.println("Starting deserialization of file: " + filePath);
        try (RandomAccessFile file = new RandomAccessFile(filePath, "r")) {
            // Verify file magic number
            byte[] magic = new byte[4];
            int bytesRead = file.read(magic);
            System.out.println("Read " + bytesRead + " bytes for magic number: " + Arrays.toString(magic));
            if (!Arrays.equals(magic, PARQUET_MAGIC)) {
                throw new IOException("Invalid Parquet file: incorrect magic number");
            }

            // Read file metadata
            MessageType schema = readFileMetadata(file);

            // Read row groups from the start of the file after the magic number
            file.seek(4);
            List<Map<String, Object>> records = readRowGroups(file, schema);

            // Verify file footer
            file.seek(file.length() - 4);
            file.read(magic);
            if (!Arrays.equals(magic, PARQUET_MAGIC)) {
                throw new IOException("Invalid Parquet file: incorrect footer magic number");
            }

            return records;
        }
    }

    private MessageType readFileMetadata(RandomAccessFile file) throws IOException {
        System.out.println("Reading file metadata...");
        try {
            // Get file size
            long fileSize = file.length();

            // Find metadata offset (stored in last 8 bytes before footer magic)
            file.seek(fileSize - 12); // 4 bytes magic + 8 bytes offset
            long metadataOffset = file.readLong();
            System.out.println("File size: " + fileSize + ", metadata offset: " + metadataOffset);

            // Read metadata
            file.seek(metadataOffset);
            System.out.println("Seeking to metadata offset: " + metadataOffset);

            // Read compression codec
            int codecValue = file.readInt();
            compressionCodec = CompressionCodec.values()[codecValue];
            System.out.println("Compression codec: " + compressionCodec);

            // Read schema name length
            int nameLength = file.readInt();
            System.out.println("Schema name length: " + nameLength);
            byte[] nameBytes = new byte[nameLength];
            int bytesRead = file.read(nameBytes);
            String schemaName = new String(nameBytes);
            System.out.println("Schema name: " + schemaName);

            // Read number of fields
            int fieldCount = file.readInt();

            // Build schema
            MessageType.Builder builder = new MessageType.Builder(schemaName);
            for (int i = 0; i < fieldCount; i++) {
                // Read field name
                int fieldNameLength = file.readInt();
                byte[] fieldNameBytes = new byte[fieldNameLength];
                file.read(fieldNameBytes);
                String fieldName = new String(fieldNameBytes);

                // Read field type
                int typeValue = file.readInt();
                Type type = Type.fromValue(typeValue);

                // Read repetition
                int repetitionValue = file.readInt();
                Repetition repetition = Repetition.fromValue(repetitionValue);

                // Read original type (if present)
                int originalTypeValue = file.readInt();
                OriginalType originalType = originalTypeValue >= 0 ? OriginalType.fromValue(originalTypeValue) : null;

                // Add field to schema
                if (originalType != null) {
                    builder.addField(fieldName, type, repetition, originalType);
                } else {
                    builder.addField(fieldName, type, repetition);
                }
            }

            MessageType schema = builder.build();
            System.out.println("Built schema: " + schema);
            return schema;
        } catch (Exception e) {
            System.out.println("Error reading metadata: " + e);
            throw e;
        }
    }

    private List<Map<String, Object>> readRowGroups(RandomAccessFile file, MessageType schema) throws IOException {
        System.out.println("Reading row groups...");

        // Read row group count
        int rowGroupCount = file.readInt();
        System.out.println("Row group count: " + rowGroupCount);

        List<Map<String, Object>> records = new ArrayList<>();

        // Read each row group
        for (int i = 0; i < rowGroupCount; i++) {
            // Read record count in this row group
            int recordCount = file.readInt();
            System.out.println("Record count in row group " + i + ": " + recordCount);

            // Initialize records for this row group
            for (int j = 0; j < recordCount; j++) {
                records.add(new HashMap<>());
            }

            // Read each field as a column chunk
            for (Field field : schema.getFields()) {
                System.out.println("Reading field: " + field.getName());
                
                // Read uncompressed size
                int uncompressedSize = file.readInt();
                System.out.println("Uncompressed size: " + uncompressedSize);

                // Read compressed size
                int compressedSize = file.readInt();
                System.out.println("Compressed size: " + compressedSize);

                // Read compressed data
                byte[] compressedData = new byte[compressedSize];
                file.readFully(compressedData);

                // Decompress if needed
                ByteBuffer dataBuffer;
                if (compressionCodec != CompressionCodec.UNCOMPRESSED) {
                    Compressor compressor = CompressorFactory.getCompressor(compressionCodec);
                    dataBuffer = compressor.decompress(ByteBuffer.wrap(compressedData), uncompressedSize);
                } else {
                    dataBuffer = ByteBuffer.wrap(compressedData);
                }

                // Read values for each record
                for (int j = 0; j < recordCount; j++) {
                    Object value = readFieldValueFromBuffer(dataBuffer, field);
                    records.get(j + (i * recordCount)).put(field.getName(), value);
                }
            }
        }

        return records;
    }

    private Object readFieldValueFromBuffer(ByteBuffer buffer, Field field) {
        // Check if field is null
        boolean isNull = field.getRepetition() == Repetition.OPTIONAL && buffer.get() == 1;
        if (isNull) {
            return null;
        }

        // Handle repeated fields
        if (field.getRepetition() == Repetition.REPEATED) {
            int count = buffer.getInt();
            List<Object> values = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                values.add(readSingleValueFromBuffer(buffer, field));
            }
            return values;
        }

        return readSingleValueFromBuffer(buffer, field);
    }

    private Object readSingleValueFromBuffer(ByteBuffer buffer, Field field) {
        switch (field.getType()) {
            case INT32:
                return buffer.getInt();
            case INT64:
                return buffer.getLong();
            case BINARY:
                int length = buffer.getInt();
                byte[] bytes = new byte[length];
                buffer.get(bytes);
                
                if (field.getOriginalType() == OriginalType.UTF8) {
                    return new String(bytes);
                } else {
                    return bytes;
                }
            default:
                throw new RuntimeException("Unsupported type: " + field.getType());
        }
    }
}
