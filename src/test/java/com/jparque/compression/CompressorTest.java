package com.jparque.compression;

import com.jparque.columnar.chunk.CompressionCodec;
import com.jparque.common.compression.Compressor;
import com.jparque.common.compression.CompressorFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import static org.assertj.core.api.Assertions.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

class CompressorTest {
    private static final String TEST_STRING = "This is a test string that should compress well " +
        "because it contains repeated patterns. This is a test string that should compress well " +
        "because it contains repeated patterns.";

    @ParameterizedTest
    @EnumSource(value = CompressionCodec.class, 
                names = {"SNAPPY", "GZIP", "ZSTD"})
    void shouldCompressAndDecompress(CompressionCodec codec) {
        // Create test data
        byte[] data = TEST_STRING.getBytes(StandardCharsets.UTF_8);
        ByteBuffer input = ByteBuffer.wrap(data);
        int originalSize = input.remaining();

        // Get compressor for codec
        Compressor compressor = CompressorFactory.getCompressor(codec);

        // Compress data
        ByteBuffer compressed = compressor.compress(input);
        
        // Verify compression actually happened (should be less than or equal)
        System.out.println("Codec: " + codec);
        System.out.println("Original size: " + originalSize);
        System.out.println("Compressed size: " + compressed.remaining());
        assertThat(compressed.remaining()).isLessThanOrEqualTo(originalSize);

        // Decompress data
        ByteBuffer decompressed = compressor.decompress(compressed, data.length);

        // Verify decompressed data matches original
        byte[] result = new byte[decompressed.remaining()];
        decompressed.get(result);
        assertThat(new String(result, StandardCharsets.UTF_8)).isEqualTo(TEST_STRING);
    }

    @Test
    void shouldHandleUncompressedData() {
        // Create test data
        byte[] data = TEST_STRING.getBytes(StandardCharsets.UTF_8);
        ByteBuffer input = ByteBuffer.wrap(data);

        // Get uncompressed codec
        Compressor compressor = CompressorFactory.getCompressor(CompressionCodec.UNCOMPRESSED);

        // "Compress" data (should just return a copy)
        ByteBuffer compressed = compressor.compress(input);
        
        // Verify no compression happened
        assertThat(compressed.remaining()).isEqualTo(input.remaining());

        // "Decompress" data (should just return a copy)
        ByteBuffer decompressed = compressor.decompress(compressed, data.length);

        // Verify data matches original
        byte[] result = new byte[decompressed.remaining()];
        decompressed.get(result);
        assertThat(new String(result, StandardCharsets.UTF_8)).isEqualTo(TEST_STRING);
    }

    @Test
    void shouldThrowExceptionForUnsupportedCodec() {
        assertThatThrownBy(() -> 
            CompressorFactory.getCompressor(CompressionCodec.LZO)
        ).isInstanceOf(UnsupportedOperationException.class)
         .hasMessageContaining("Compression codec not supported");
    }
}
