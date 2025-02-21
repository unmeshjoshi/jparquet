package com.jparque.common.compression;

import java.nio.ByteBuffer;

/**
 * A "compressor" that simply copies the data without any compression.
 */
public class UncompressedCompressor implements Compressor {
    @Override
    public ByteBuffer compress(ByteBuffer uncompressed) {
        // Create a copy of the input buffer
        ByteBuffer copy = ByteBuffer.allocate(uncompressed.remaining());
        copy.put(uncompressed.duplicate());
        copy.flip();
        return copy;
    }

    @Override
    public ByteBuffer decompress(ByteBuffer compressed, int uncompressedLength) {
        // Create a copy of the input buffer
        ByteBuffer copy = ByteBuffer.allocate(compressed.remaining());
        copy.put(compressed.duplicate());
        copy.flip();
        return copy;
    }
}
