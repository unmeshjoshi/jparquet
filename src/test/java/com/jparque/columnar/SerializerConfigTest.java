package com.jparque.columnar;

import com.jparque.columnar.chunk.CompressionCodec;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class SerializerConfigTest {
    @Test
    void shouldUseDefaultValues() {
        SerializerConfig config = new SerializerConfig.Builder().build();
        assertThat(config.getCompressionCodec()).isEqualTo(CompressionCodec.UNCOMPRESSED);
        assertThat(config.getRowGroupSize()).isEqualTo(128 * 1024 * 1024); // 128MB
    }

    @Test
    void shouldUseCustomValues() {
        SerializerConfig config = new SerializerConfig.Builder()
            .setCompressionCodec(CompressionCodec.SNAPPY)
            .setRowGroupSize(64 * 1024 * 1024) // 64MB
            .build();

        assertThat(config.getCompressionCodec()).isEqualTo(CompressionCodec.SNAPPY);
        assertThat(config.getRowGroupSize()).isEqualTo(64 * 1024 * 1024);
    }
}
