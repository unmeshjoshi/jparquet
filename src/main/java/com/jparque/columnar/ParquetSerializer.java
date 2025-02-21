package com.jparque.columnar;

import com.jparque.common.schema.*;
import com.jparque.columnar.chunk.CompressionCodec;
import com.jparque.columnar.SerializerConfig;
import com.jparque.common.compression.Compressor;
import com.jparque.common.compression.CompressorFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * Serializes data into Parquet format.
 * This is a custom implementation of the Parquet format specification.
 */
public class ParquetSerializer {
    private static final byte[] PARQUET_MAGIC = new byte[] {'P', 'A', 'R', '1'};
    private final MessageType schema;
    private final SerializerConfig config;
    private long metadataOffset;

    public ParquetSerializer(MessageType schema) {
        this(schema, new SerializerConfig.Builder().build());
    }

    public ParquetSerializer(MessageType schema, SerializerConfig config) {
        this.schema = schema;
        this.config = config;
    }

    /**
     * Serializes a list of records into a Parquet file.
     *
     * @param records List of records to serialize
     * @param filePath Path to the output file
     * @throws IOException if there's an error writing to the file
     */
    public void serialize(List<Map<String, Object>> records, String filePath) throws IOException {
        validateRecords(records);

        try (RandomAccessFile file = new RandomAccessFile(filePath, "rw")) {
            // Write file header
            writeFileHeader(file);

            // Write row groups
            writeRowGroups(file, records);

            // Write file metadata
            writeFileMetadata(file);

            // Write file footer
            writeFileFooter(file);
        }
    }

    private void validateRecords(List<Map<String, Object>> records) {
        for (Map<String, Object> record : records) {
            validateRecord(record);
        }
    }

    private void validateRecord(Map<String, Object> record) {
        for (Field field : schema.getFields()) {
            String fieldName = field.getName();
            Object value = record.get(fieldName);

            if (field.getRepetition() == Repetition.REQUIRED && value == null) {
                throw new IllegalArgumentException("Missing required field: " + fieldName);
            }

            if (value != null) {
                validateFieldType(field, value);
            }
        }
    }

    private void validateFieldType(Field field, Object value) {
        if (field.getRepetition() == Repetition.REPEATED) {
            if (!(value instanceof List)) {
                throw new IllegalArgumentException(
                    String.format("Invalid type for repeated field '%s': expected List, got %s",
                        field.getName(), value.getClass().getSimpleName()));
            }
            // Validate each element in the list
            for (Object element : (List<?>) value) {
                validateSingleValue(field, element);
            }
        } else {
            validateSingleValue(field, value);
        }
    }

    private void validateSingleValue(Field field, Object value) {
        switch (field.getType()) {
            case INT32:
                if (!(value instanceof Integer)) {
                    throw new IllegalArgumentException(
                        String.format("Invalid type for field '%s': expected Integer, got %s",
                            field.getName(), value.getClass().getSimpleName()));
                }
                break;
            case INT64:
                if (!(value instanceof Long)) {
                    throw new IllegalArgumentException(
                        String.format("Invalid type for field '%s': expected Long, got %s",
                            field.getName(), value.getClass().getSimpleName()));
                }
                break;
            case BINARY:
                if (field.getOriginalType() == OriginalType.UTF8) {
                    if (!(value instanceof String)) {
                        throw new IllegalArgumentException(
                            String.format("Invalid type for field '%s': expected String, got %s",
                                field.getName(), value.getClass().getSimpleName()));
                    }
                }
                break;
            // Add other type validations as needed
            default:
                throw new IllegalArgumentException(
                    String.format("Unsupported type for field '%s': %s",
                        field.getName(), field.getType()));
        }
    }

    private void writeFileHeader(RandomAccessFile file) throws IOException {
        file.write(PARQUET_MAGIC);
    }

    private void writeRowGroups(RandomAccessFile file, List<Map<String, Object>> records) throws IOException {
        // Write row group count
        ByteBuffer countBuffer = ByteBuffer.allocate(4);
        countBuffer.putInt(1); // For now, we'll put all records in one row group
        countBuffer.flip();
        file.write(countBuffer.array());
        
        // Write record count
        ByteBuffer recordCountBuffer = ByteBuffer.allocate(4);
        recordCountBuffer.putInt(records.size());
        recordCountBuffer.flip();
        file.write(recordCountBuffer.array());
        
        // Write each field as a column chunk
        for (Field field : schema.getFields()) {
            // First collect all values for this field
            ByteBuffer columnBuffer = ByteBuffer.allocate(1024 * 1024); // 1MB initial size
            
            // Write each record's value for this field
            for (Map<String, Object> record : records) {
                Object value = record.get(field.getName());
                writeFieldValueToBuffer(columnBuffer, field, value);
            }
            
            // Prepare the column chunk for writing
            columnBuffer.flip();
            int uncompressedSize = columnBuffer.remaining();
            
            // Write uncompressed size
            ByteBuffer uncompressedSizeBuffer = ByteBuffer.allocate(4);
            uncompressedSizeBuffer.putInt(uncompressedSize);
            uncompressedSizeBuffer.flip();
            file.write(uncompressedSizeBuffer.array());
            
            // Compress the entire column chunk if needed
            ByteBuffer compressedBuffer = columnBuffer;
            if (config.getCompressionCodec() != CompressionCodec.UNCOMPRESSED) {
                Compressor compressor = CompressorFactory.getCompressor(config.getCompressionCodec());
                compressedBuffer = compressor.compress(columnBuffer);
            }
            
            // Write compressed size
            ByteBuffer compressedSizeBuffer = ByteBuffer.allocate(4);
            compressedSizeBuffer.putInt(compressedBuffer.remaining());
            compressedSizeBuffer.flip();
            file.write(compressedSizeBuffer.array());
            
            // Write the data
            byte[] data = new byte[compressedBuffer.remaining()];
            compressedBuffer.get(data);
            file.write(data);
        }
    }
    
    private void writeFieldValueToBuffer(ByteBuffer buffer, Field field, Object value) {
        // Handle null values for optional fields
        if (field.getRepetition() == Repetition.OPTIONAL) {
            buffer.put((byte) (value == null ? 1 : 0));
            if (value == null) {
                return;
            }
        }
        
        // Handle repeated fields
        if (field.getRepetition() == Repetition.REPEATED) {
            List<?> values = value != null ? (List<?>) value : List.of();
            buffer.putInt(values.size());
            for (Object item : values) {
                writeSingleValueToBuffer(buffer, field, item);
            }
            return;
        }
        
        writeSingleValueToBuffer(buffer, field, value);
    }
    
    private void writeSingleValueToBuffer(ByteBuffer buffer, Field field, Object value) {
        switch (field.getType()) {
            case INT32:
                buffer.putInt((Integer) value);
                break;
                
            case INT64:
                buffer.putLong((Long) value);
                break;
                
            case BINARY:
                byte[] bytes;
                if (field.getOriginalType() == OriginalType.UTF8) {
                    bytes = ((String) value).getBytes();
                } else {
                    bytes = (byte[]) value;
                }
                
                buffer.putInt(bytes.length);
                buffer.put(bytes);
                break;
                
            default:
                throw new RuntimeException("Unsupported type: " + field.getType());
        }
    }

    private void writeFileMetadata(RandomAccessFile file) throws IOException {
        metadataOffset = file.getFilePointer();
        
        // Write compression codec
        ByteBuffer codecBuffer = ByteBuffer.allocate(4);
        codecBuffer.putInt(config.getCompressionCodec().ordinal());
        codecBuffer.flip();
        file.write(codecBuffer.array());
        
        // Write schema name
        String schemaName = schema.getName();
        if (schemaName == null || schemaName.isEmpty()) {
            schemaName = "default";
        }
        byte[] nameBytes = schemaName.getBytes();
        ByteBuffer nameBuffer = ByteBuffer.allocate(4 + nameBytes.length);
        nameBuffer.putInt(nameBytes.length);
        nameBuffer.put(nameBytes);
        nameBuffer.flip();
        file.write(nameBuffer.array());
        
        // Write number of fields
        List<Field> fields = schema.getFields();
        ByteBuffer fieldCountBuffer = ByteBuffer.allocate(4);
        fieldCountBuffer.putInt(fields.size());
        fieldCountBuffer.flip();
        file.write(fieldCountBuffer.array());
        
        // Write each field
        for (Field field : fields) {
            // Write field name
            byte[] fieldNameBytes = field.getName().getBytes();
            ByteBuffer fieldNameBuffer = ByteBuffer.allocate(4 + fieldNameBytes.length);
            fieldNameBuffer.putInt(fieldNameBytes.length);
            fieldNameBuffer.put(fieldNameBytes);
            fieldNameBuffer.flip();
            file.write(fieldNameBuffer.array());
            
            // Write field type
            ByteBuffer typeBuffer = ByteBuffer.allocate(4);
            typeBuffer.putInt(field.getType().getValue());
            typeBuffer.flip();
            file.write(typeBuffer.array());
            
            // Write field repetition
            ByteBuffer repetitionBuffer = ByteBuffer.allocate(4);
            repetitionBuffer.putInt(field.getRepetition().getValue());
            repetitionBuffer.flip();
            file.write(repetitionBuffer.array());
            
            // Write field original type if present
            OriginalType originalType = field.getOriginalType();
            ByteBuffer originalTypeBuffer = ByteBuffer.allocate(4);
            originalTypeBuffer.putInt(originalType != null ? originalType.getValue() : -1);
            originalTypeBuffer.flip();
            file.write(originalTypeBuffer.array());

        }
        
    }

    private void writeFileFooter(RandomAccessFile file) throws IOException {
        // Write metadata offset at end of file
        ByteBuffer metadataOffsetBuffer = ByteBuffer.allocate(8);
        metadataOffsetBuffer.putLong(metadataOffset);
        metadataOffsetBuffer.flip();
        file.write(metadataOffsetBuffer.array());

        // Write footer magic
        file.write(PARQUET_MAGIC);
    }
}
