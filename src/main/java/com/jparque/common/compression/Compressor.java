package com.jparque.common.compression;

import java.nio.ByteBuffer;

/**
 * Interface for compression/decompression operations.
 */
public interface Compressor {
    /**
     * Compresses the given data.
     *
     * @param uncompressed The data to compress
     * @return A ByteBuffer containing the compressed data
     */
    ByteBuffer compress(ByteBuffer uncompressed);

    /**
     * Decompresses the given data.
     *
     * @param compressed The data to decompress
     * @param uncompressedLength The expected length of the uncompressed data
     * @return A ByteBuffer containing the decompressed data
     */
    ByteBuffer decompress(ByteBuffer compressed, int uncompressedLength);
}
