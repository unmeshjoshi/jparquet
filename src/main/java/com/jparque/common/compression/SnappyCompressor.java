package com.jparque.common.compression;

import org.xerial.snappy.Snappy;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Compressor implementation using Snappy compression.
 */
public class SnappyCompressor implements Compressor {
    @Override
    public ByteBuffer compress(ByteBuffer uncompressed) {
        try {
            byte[] input = new byte[uncompressed.remaining()];
            uncompressed.get(input);
            
            byte[] compressed = Snappy.compress(input);
            return ByteBuffer.wrap(compressed);
        } catch (IOException e) {
            throw new RuntimeException("Failed to compress data with Snappy", e);
        }
    }

    @Override
    public ByteBuffer decompress(ByteBuffer compressed, int uncompressedLength) {
        try {
            byte[] input = new byte[compressed.remaining()];
            compressed.get(input);
            
            byte[] decompressed = Snappy.uncompress(input);
            return ByteBuffer.wrap(decompressed);
        } catch (IOException e) {
            throw new RuntimeException("Failed to decompress data with Snappy", e);
        }
    }
}
