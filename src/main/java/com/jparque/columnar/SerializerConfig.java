package com.jparque.columnar;

import com.jparque.columnar.chunk.CompressionCodec;

/**
 * Configuration for Parquet serialization.
 */
public class SerializerConfig {
    private final CompressionCodec compressionCodec;
    private final int rowGroupSize;

    private SerializerConfig(Builder builder) {
        this.compressionCodec = builder.compressionCodec;
        this.rowGroupSize = builder.rowGroupSize;
    }

    public CompressionCodec getCompressionCodec() {
        return compressionCodec;
    }

    public int getRowGroupSize() {
        return rowGroupSize;
    }

    public static class Builder {
        private CompressionCodec compressionCodec = CompressionCodec.UNCOMPRESSED;
        private int rowGroupSize = 128 * 1024 * 1024; // Default to 128MB

        public Builder setCompressionCodec(CompressionCodec compressionCodec) {
            this.compressionCodec = compressionCodec;
            return this;
        }

        public Builder setRowGroupSize(int rowGroupSize) {
            this.rowGroupSize = rowGroupSize;
            return this;
        }

        public SerializerConfig build() {
            return new SerializerConfig(this);
        }
    }
}
