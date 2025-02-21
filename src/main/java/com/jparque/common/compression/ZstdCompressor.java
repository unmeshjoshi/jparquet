package com.jparque.common.compression;

import com.github.luben.zstd.Zstd;
import java.nio.ByteBuffer;

/**
 * Compressor implementation using Zstandard compression.
 */
public class ZstdCompressor implements Compressor {
    private static final int COMPRESSION_LEVEL = 3; // Default compression level

    @Override
    public ByteBuffer compress(ByteBuffer uncompressed) {
        byte[] input = new byte[uncompressed.remaining()];
        uncompressed.get(input);
        
        long maxCompressedSize = Zstd.compressBound(input.length);
        byte[] compressed = new byte[(int) maxCompressedSize];
        
        long compressedSize = Zstd.compressByteArray(
            compressed, 0, compressed.length,
            input, 0, input.length,
            COMPRESSION_LEVEL
        );
        
        if (Zstd.isError(compressedSize)) {
            throw new RuntimeException(
                "Failed to compress data with Zstd: " + 
                Zstd.getErrorName(compressedSize)
            );
        }
        
        return ByteBuffer.wrap(compressed, 0, (int) compressedSize);
    }

    @Override
    public ByteBuffer decompress(ByteBuffer compressed, int uncompressedLength) {
        byte[] input = new byte[compressed.remaining()];
        compressed.get(input);
        
        byte[] decompressed = new byte[uncompressedLength];
        long decompressedSize = Zstd.decompressByteArray(
            decompressed, 0, decompressed.length,
            input, 0, input.length
        );
        
        if (Zstd.isError(decompressedSize)) {
            throw new RuntimeException(
                "Failed to decompress data with Zstd: " + 
                Zstd.getErrorName(decompressedSize)
            );
        }
        
        if (decompressedSize != uncompressedLength) {
            throw new RuntimeException(
                "Decompressed size mismatch. Expected: " + uncompressedLength +
                ", Got: " + decompressedSize
            );
        }
        
        return ByteBuffer.wrap(decompressed);
    }
}
