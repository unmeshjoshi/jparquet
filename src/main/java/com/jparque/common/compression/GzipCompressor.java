package com.jparque.common.compression;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.GZIPOutputStream;
import java.util.zip.GZIPInputStream;
import java.io.ByteArrayInputStream;

/**
 * Compressor implementation using GZIP compression.
 */
public class GzipCompressor implements Compressor {
    @Override
    public ByteBuffer compress(ByteBuffer uncompressed) {
        try {
            byte[] input = new byte[uncompressed.remaining()];
            uncompressed.get(input);
            
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (GZIPOutputStream gzos = new GZIPOutputStream(baos)) {
                gzos.write(input);
            }
            
            return ByteBuffer.wrap(baos.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException("Failed to compress data with GZIP", e);
        }
    }

    @Override
    public ByteBuffer decompress(ByteBuffer compressed, int uncompressedLength) {
        try {
            byte[] input = new byte[compressed.remaining()];
            compressed.get(input);
            
            ByteArrayInputStream bais = new ByteArrayInputStream(input);
            GZIPInputStream gzis = new GZIPInputStream(bais);
            
            byte[] output = new byte[uncompressedLength];
            int bytesRead = gzis.read(output);
            
            if (bytesRead != uncompressedLength) {
                throw new RuntimeException(
                    "Decompressed size mismatch. Expected: " + uncompressedLength +
                    ", Got: " + bytesRead
                );
            }
            
            return ByteBuffer.wrap(output);
        } catch (IOException e) {
            throw new RuntimeException("Failed to decompress data with GZIP", e);
        }
    }
}
