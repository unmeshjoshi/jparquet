package com.jparque.common;

import com.jparque.columnar.rowgroup.RowGroup;
import com.jparque.common.schema.Field;
import com.jparque.common.schema.MessageType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Represents the metadata of a Parquet file.
 * This includes the schema, version, creation info, and row group metadata.
 */
public class FileMetadata {
    private final MessageType schema;
    private final int version;
    private final String createdBy;
    private final List<RowGroup> rowGroups;

    private FileMetadata(Builder builder) {
        this.schema = builder.schema;
        this.version = builder.version;
        this.createdBy = builder.createdBy;
        this.rowGroups = Collections.unmodifiableList(new ArrayList<>(builder.rowGroups));
    }

    /**
     * Writes this metadata to the given file channel.
     * 
     * @param channel The file channel to write to
     * @return The number of bytes written
     * @throws IOException If an I/O error occurs
     */
    public long writeTo(FileChannel channel) throws IOException {
        long startPosition = channel.position();

        // Calculate buffer size
        int bufferSize = 4; // version
        
        // Schema size
        bufferSize += 4; // number of fields
        for (Field field : schema.getFields()) {
            bufferSize += 8; // type + repetition
            if (field.getOriginalType() != null) {
                bufferSize += 4; // original type
            }
            bufferSize += 4 + field.getName().getBytes().length; // name length + name
        }

        // Created by size
        bufferSize += 4 + createdBy.getBytes().length;

        // Row groups metadata size
        bufferSize += 32; // numRowGroups(8) + rowCount(8) + totalSize(8) + startOffset(8)

        // Allocate and fill buffer
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);

        // Write version
        buffer.putInt(version);

        // Write schema
        buffer.putInt(schema.getFields().size());
        for (Field field : schema.getFields()) {
            buffer.putInt(field.getType().getValue());
            buffer.putInt(field.getRepetition().getValue());
            if (field.getOriginalType() != null) {
                buffer.putInt(field.getOriginalType().getValue());
            }
            byte[] nameBytes = field.getName().getBytes();
            buffer.putInt(nameBytes.length);
            buffer.put(nameBytes);
        }

        // Write created by
        byte[] createdByBytes = createdBy.getBytes();
        buffer.putInt(createdByBytes.length);
        buffer.put(createdByBytes);

        // Write row groups metadata
        buffer.putLong(rowGroups.size());
        for (RowGroup rowGroup : rowGroups) {
            buffer.putLong(rowGroup.getRowCount());
            buffer.putLong(rowGroup.getTotalByteSize());
            buffer.putLong(0); // start offset, for now assuming 0
        }

        buffer.flip();
        return channel.write(buffer);
    }

    public MessageType getSchema() {
        return schema;
    }

    public int getVersion() {
        return version;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public List<RowGroup> getRowGroups() {
        return rowGroups;
    }

    public static class Builder {
        private MessageType schema;
        private int version = 1;
        private String createdBy = "JParque";
        private final List<RowGroup> rowGroups = new ArrayList<>();

        public Builder setSchema(MessageType schema) {
            this.schema = schema;
            return this;
        }

        public Builder setVersion(int version) {
            this.version = version;
            return this;
        }

        public Builder setCreatedBy(String createdBy) {
            this.createdBy = createdBy;
            return this;
        }

        public Builder addRowGroup(RowGroup rowGroup) {
            rowGroups.add(rowGroup);
            return this;
        }

        public FileMetadata build() {
            if (schema == null) {
                throw new IllegalStateException("Schema must be set");
            }
            return new FileMetadata(this);
        }
    }
}
