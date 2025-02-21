package com.jparque.chunk;

import com.jparque.columnar.chunk.ColumnChunk;
import com.jparque.columnar.chunk.CompressionCodec;
import com.jparque.columnar.page.DataPage;
import com.jparque.columnar.page.Encoding;
import com.jparque.columnar.page.Statistics;
import com.jparque.common.schema.Type;
import com.jparque.common.compression.Compressor;
import com.jparque.common.compression.CompressorFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import static org.assertj.core.api.Assertions.*;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

class ColumnChunkTest {
    private long calculateTotalSize(DataPage page, int compressedSize) {
        // Total size = chunk metadata (24) + page header + compressed data size
        return 24 + page.getHeaderSize() + compressedSize;
    }
    private static final byte[] TEST_DATA = new byte[1024];
    
    static {
        // Fill test data with repeating pattern to make it compressible
        for (int i = 0; i < TEST_DATA.length; i++) {
            TEST_DATA[i] = (byte)(i % 4);
        }
    }
    @ParameterizedTest
    @EnumSource(value = CompressionCodec.class, names = {"UNCOMPRESSED", "SNAPPY", "GZIP", "ZSTD"})
    void shouldWriteColumnChunkWithCompression(CompressionCodec codec) throws Exception {
        // Create a temporary file for testing
        File tempFile = File.createTempFile("column-chunk-test", ".parquet");
        tempFile.deleteOnExit();

        // Create test data with repeating pattern to make it compressible
        ByteBuffer buffer = ByteBuffer.wrap(TEST_DATA);
        
        // Create statistics for the page
        ByteBuffer minValue = ByteBuffer.wrap(new byte[] { 1 });
        ByteBuffer maxValue = ByteBuffer.wrap(new byte[] { 4 });
        Statistics pageStats = new Statistics(minValue, maxValue, 0, TEST_DATA.length);

        // Create a data page
        DataPage page = new DataPage(
            buffer,
            TEST_DATA.length, // value count
            Encoding.PLAIN,
            Encoding.RLE,
            Encoding.BIT_PACKED,
            pageStats
        );

        // Compress the data to get the compressed size
        Compressor compressor = CompressorFactory.getCompressor(codec);
        ByteBuffer compressed = compressor.compress(buffer.duplicate());
        int compressedSize = compressed.remaining();
        page.setCompressedSize(compressedSize);

        // Create column chunk
        ColumnChunk chunk = new ColumnChunk.Builder()
            .setType(Type.INT32)
            .setCodec(codec)
            .setPath("test_column")
            .setValueCount(TEST_DATA.length)
            .setTotalSize(calculateTotalSize(page, compressedSize))
            .setFirstDataPageOffset(0)
            .addPage(page)
            .build();

        // Write chunk to file
        try (RandomAccessFile file = new RandomAccessFile(tempFile, "rw");
             FileChannel channel = file.getChannel()) {
            
            long bytesWritten = chunk.writeTo(channel);

            // Verify bytes written matches total size
            assertThat(bytesWritten).isEqualTo(chunk.getTotalSize());

            // Read back and verify contents
            ByteBuffer readBuffer = ByteBuffer.allocate((int) bytesWritten);
            channel.position(0);
            channel.read(readBuffer);
            readBuffer.flip();

            // Verify chunk metadata
            assertThat(readBuffer.getInt()).isEqualTo(Type.INT32.getValue()); // type
            assertThat(readBuffer.getInt()).isEqualTo(codec.getValue()); // codec
            assertThat(readBuffer.getLong()).isEqualTo(TEST_DATA.length); // value count
            assertThat(readBuffer.getLong()).isEqualTo(chunk.getTotalSize()); // total size
            
            // Verify page data
            assertThat(readBuffer.get()).isEqualTo((byte) page.getType().getValue());
            assertThat(readBuffer.getInt()).isEqualTo(page.getUncompressedSize());
            assertThat(readBuffer.getInt()).isEqualTo(page.getCompressedSize());
            assertThat(readBuffer.getInt()).isEqualTo(page.getValueCount());
            assertThat(readBuffer.getInt()).isEqualTo(page.getEncoding().getValue());
            assertThat(readBuffer.getInt()).isEqualTo(page.getDefinitionLevelEncoding().getValue());
            assertThat(readBuffer.getInt()).isEqualTo(page.getRepetitionLevelEncoding().getValue());

            // Verify statistics
            assertThat(readBuffer.getInt()).isEqualTo(1); // min value length
            assertThat(readBuffer.get()).isEqualTo((byte) 1); // min value
            assertThat(readBuffer.getInt()).isEqualTo(1); // max value length
            assertThat(readBuffer.get()).isEqualTo((byte) 4); // max value
            assertThat(readBuffer.getLong()).isEqualTo(0); // null count
            assertThat(readBuffer.getLong()).isEqualTo(TEST_DATA.length); // distinct count

            // Get compressed data
            byte[] compressedData = new byte[readBuffer.remaining()];
            readBuffer.get(compressedData);
            
            // Decompress and verify
            ByteBuffer compressedBuffer = ByteBuffer.wrap(compressedData);
            ByteBuffer decompressedBuffer = compressor.decompress(compressedBuffer, TEST_DATA.length);
            
            byte[] decompressedData = new byte[TEST_DATA.length];
            decompressedBuffer.get(decompressedData);
            assertThat(decompressedData).isEqualTo(TEST_DATA);
            
            // Verify compression actually happened for non-UNCOMPRESSED codecs
            if (codec != CompressionCodec.UNCOMPRESSED) {
                assertThat(compressedData.length).isLessThan(TEST_DATA.length);
            }
        }
    }
}
