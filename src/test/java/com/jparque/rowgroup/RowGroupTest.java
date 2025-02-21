package com.jparque.rowgroup;

import com.jparque.columnar.chunk.ColumnChunk;
import com.jparque.columnar.chunk.CompressionCodec;
import com.jparque.columnar.page.DataPage;
import com.jparque.columnar.page.Encoding;
import com.jparque.columnar.page.Statistics;
import com.jparque.columnar.rowgroup.RowGroup;
import com.jparque.common.schema.Type;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

class RowGroupTest {
    @Test
    void shouldWriteRowGroupCorrectly() throws Exception {
        // Create a temporary file for testing
        File tempFile = File.createTempFile("row-group-test", ".parquet");
        tempFile.deleteOnExit();

        // Create test data for first column (INT32)
        byte[] intData = new byte[] { 1, 0, 0, 0 }; // 1 as a 4-byte integer
        ByteBuffer intBuffer = ByteBuffer.wrap(intData);
        Statistics intStats = new Statistics(
            ByteBuffer.wrap(new byte[] { 1, 0, 0, 0 }), // min = 1
            ByteBuffer.wrap(new byte[] { 1, 0, 0, 0 }), // max = 1
            0, // null count
            1  // distinct count
        );
        DataPage intPage = new DataPage(
            intBuffer,
            1, // value count
            Encoding.PLAIN,
            Encoding.RLE,
            Encoding.BIT_PACKED,
            intStats
        );
        ColumnChunk intColumn = new ColumnChunk.Builder()
            .setType(Type.INT32)
            .setCodec(CompressionCodec.UNCOMPRESSED)
            .setPath("age")
            .setValueCount(1)
            .setTotalSize(24 + intPage.getHeaderSize() + intPage.getData().remaining())
            .setFirstDataPageOffset(0)
            .addPage(intPage)
            .build();

        // Create test data for second column (BINARY/String)
        byte[] strData = "test".getBytes();
        ByteBuffer strBuffer = ByteBuffer.wrap(strData);
        Statistics strStats = new Statistics(
            ByteBuffer.wrap("test".getBytes()),
            ByteBuffer.wrap("test".getBytes()),
            0,
            1
        );
        DataPage strPage = new DataPage(
            strBuffer,
            1,
            Encoding.PLAIN,
            Encoding.RLE,
            Encoding.BIT_PACKED,
            strStats
        );
        ColumnChunk strColumn = new ColumnChunk.Builder()
            .setType(Type.BINARY)
            .setCodec(CompressionCodec.UNCOMPRESSED)
            .setPath("name")
            .setValueCount(1)
            .setTotalSize(24 + strPage.getHeaderSize() + strPage.getData().remaining())
            .setFirstDataPageOffset(0)
            .addPage(strPage)
            .build();

        // Create row group
        RowGroup rowGroup = new RowGroup.Builder()
            .addColumn(intColumn)
            .addColumn(strColumn)
            .setRowCount(1)
            .build();

        // Write row group to file
        try (RandomAccessFile file = new RandomAccessFile(tempFile, "rw");
             FileChannel channel = file.getChannel()) {
            
            long bytesWritten = rowGroup.writeTo(channel);

            // Verify total bytes written
            assertThat(bytesWritten).isEqualTo(
                intColumn.getTotalSize() + strColumn.getTotalSize()
            );

            // Read back and verify contents
            channel.position(0);
            ByteBuffer readBuffer = ByteBuffer.allocate((int) bytesWritten);
            channel.read(readBuffer);
            readBuffer.flip();

            // Verify first column (INT32)
            assertThat(readBuffer.getInt()).isEqualTo(Type.INT32.getValue());
            assertThat(readBuffer.getInt()).isEqualTo(CompressionCodec.UNCOMPRESSED.getValue());
            assertThat(readBuffer.getLong()).isEqualTo(1); // value count
            assertThat(readBuffer.getLong()).isEqualTo(intColumn.getTotalSize());

            // Skip page header and verify int data
            skipPageHeader(readBuffer);
            byte[] readIntData = new byte[4];
            readBuffer.get(readIntData);
            assertThat(readIntData).isEqualTo(intData);

            // Verify second column (BINARY)
            assertThat(readBuffer.getInt()).isEqualTo(Type.BINARY.getValue());
            assertThat(readBuffer.getInt()).isEqualTo(CompressionCodec.UNCOMPRESSED.getValue());
            assertThat(readBuffer.getLong()).isEqualTo(1); // value count
            assertThat(readBuffer.getLong()).isEqualTo(strColumn.getTotalSize());

            // Skip page header and verify string data
            skipPageHeader(readBuffer);
            byte[] readStrData = new byte[4];
            readBuffer.get(readStrData);
            assertThat(readStrData).isEqualTo(strData);
        }
    }

    private void skipPageHeader(ByteBuffer buffer) {
        // Skip page type (1) + sizes (8) + value count (4) + encodings (12)
        buffer.position(buffer.position() + 25);
        
        // Skip statistics
        int minValueLength = buffer.getInt();
        buffer.position(buffer.position() + minValueLength);
        int maxValueLength = buffer.getInt();
        buffer.position(buffer.position() + maxValueLength);
        buffer.position(buffer.position() + 16); // null count (8) + distinct count (8)
    }
}
