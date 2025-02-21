package com.jparque.columnar.rowgroup;

import com.jparque.columnar.chunk.ColumnChunk;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Represents a row group in a Parquet file.
 * A row group is a horizontal partition of the data that contains a subset of rows.
 * Each row group contains one chunk for each column in the dataset.
 */
public class RowGroup {
    private final List<ColumnChunk> columns;
    private final long rowCount;
    private final long totalByteSize;

    private RowGroup(Builder builder) {
        this.columns = Collections.unmodifiableList(new ArrayList<>(builder.columns));
        this.rowCount = builder.rowCount;
        this.totalByteSize = builder.columns.stream()
            .mapToLong(ColumnChunk::getTotalSize)
            .sum();
    }

    /**
     * Writes this row group to the given file channel.
     * 
     * @param channel The file channel to write to
     * @return The number of bytes written
     * @throws IOException If an I/O error occurs
     */
    public long writeTo(FileChannel channel) throws IOException {
        long startPosition = channel.position();
        
        // Write each column chunk
        for (ColumnChunk column : columns) {
            column.writeTo(channel);
        }

        return channel.position() - startPosition;
    }

    public List<ColumnChunk> getColumns() {
        return columns;
    }

    public long getRowCount() {
        return rowCount;
    }

    public long getTotalByteSize() {
        return totalByteSize;
    }

    public static class Builder {
        private final List<ColumnChunk> columns = new ArrayList<>();
        private long rowCount;

        public Builder addColumn(ColumnChunk column) {
            columns.add(column);
            return this;
        }

        public Builder setRowCount(long rowCount) {
            this.rowCount = rowCount;
            return this;
        }

        public RowGroup build() {
            if (columns.isEmpty()) {
                throw new IllegalStateException("At least one column must be added");
            }
            if (rowCount <= 0) {
                throw new IllegalStateException("Row count must be positive");
            }
            return new RowGroup(this);
        }
    }
}
