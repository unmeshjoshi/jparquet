package com.jparque.common.compression;

import com.jparque.columnar.chunk.CompressionCodec;

/**
 * Factory for creating compressors based on compression codec.
 */
public class CompressorFactory {
    /**
     * Returns a compressor for the given codec.
     *
     * @param codec The compression codec to use
     * @return A compressor that implements the specified codec
     * @throws UnsupportedOperationException if the codec is not supported
     */
    public static Compressor getCompressor(CompressionCodec codec) {
        return switch (codec) {
            case UNCOMPRESSED -> new UncompressedCompressor();
            case SNAPPY -> new SnappyCompressor();
            case GZIP -> new GzipCompressor();
            case ZSTD -> new ZstdCompressor();
            default -> throw new UnsupportedOperationException(
                "Compression codec not supported: " + codec
            );
        };
    }
}
