package com.jparque.columnar.chunk;

import com.jparque.columnar.page.Page;
import com.jparque.columnar.page.DataPage;
import com.jparque.common.schema.Type;
import com.jparque.common.compression.Compressor;
import com.jparque.common.compression.CompressorFactory;
import com.jparque.columnar.chunk.CompressionCodec;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Represents a column chunk in a Parquet file.
 * A column chunk contains the data for a column within a row group.
 */
public class ColumnChunk {
    private final Type type;
    private final CompressionCodec codec;
    private final String path;
    private final long valueCount;
    private final long totalSize;
    private final long firstDataPageOffset;
    private final List<Page> pages;

    private ColumnChunk(Builder builder) {
        this.type = builder.type;
        this.codec = builder.codec;
        this.path = builder.path;
        this.valueCount = builder.valueCount;
        this.totalSize = builder.totalSize;
        this.firstDataPageOffset = builder.firstDataPageOffset;
        this.pages = Collections.unmodifiableList(new ArrayList<>(builder.pages));
    }

    public long writeTo(FileChannel channel) throws IOException {
        long startPosition = channel.position();
        
        // Write chunk metadata
        ByteBuffer metadataBuffer = ByteBuffer.allocate(24); // type(4) + codec(4) + valueCount(8) + totalSize(8)
        metadataBuffer.putInt(type.getValue());
        metadataBuffer.putInt(codec.getValue());
        metadataBuffer.putLong(valueCount);
        metadataBuffer.putLong(totalSize);
        metadataBuffer.flip();
        channel.write(metadataBuffer);

        // Write pages
        for (Page page : pages) {
            // Write page header
            ByteBuffer headerBuffer = ByteBuffer.allocate(page.getHeaderSize());
            page.writeHeader(headerBuffer);
            headerBuffer.flip();
            channel.write(headerBuffer);

            // Compress and write page data
            ByteBuffer pageData = page.getData();
            Compressor compressor = CompressorFactory.getCompressor(codec);
            ByteBuffer compressedData = compressor.compress(pageData);
            page.setCompressedSize(compressedData.remaining());
            channel.write(compressedData);
        }

        return channel.position() - startPosition;
    }

    public Type getType() {
        return type;
    }

    public CompressionCodec getCodec() {
        return codec;
    }

    public String getPath() {
        return path;
    }

    public long getValueCount() {
        return valueCount;
    }

    public long getTotalSize() {
        return totalSize;
    }

    public long getFirstDataPageOffset() {
        return firstDataPageOffset;
    }

    public List<Page> getPages() {
        return pages;
    }

    public static class Builder {
        private Type type;
        private CompressionCodec codec = CompressionCodec.UNCOMPRESSED;
        private String path;
        private long valueCount;
        private long totalSize;
        private long firstDataPageOffset;
        private final List<Page> pages = new ArrayList<>();

        public Builder setType(Type type) {
            this.type = type;
            return this;
        }

        public Builder setCodec(CompressionCodec codec) {
            this.codec = codec;
            return this;
        }

        public Builder setPath(String path) {
            this.path = path;
            return this;
        }

        public Builder setValueCount(long valueCount) {
            this.valueCount = valueCount;
            return this;
        }

        public Builder setTotalSize(long totalSize) {
            this.totalSize = totalSize;
            return this;
        }

        public Builder setFirstDataPageOffset(long firstDataPageOffset) {
            this.firstDataPageOffset = firstDataPageOffset;
            return this;
        }

        public Builder addPage(Page page) {
            this.pages.add(page);
            return this;
        }

        public ColumnChunk build() {
            if (type == null) {
                throw new IllegalStateException("Type must be set");
            }
            if (path == null) {
                throw new IllegalStateException("Path must be set");
            }
            if (pages.isEmpty()) {
                throw new IllegalStateException("At least one page must be added");
            }
            return new ColumnChunk(this);
        }
    }
}
