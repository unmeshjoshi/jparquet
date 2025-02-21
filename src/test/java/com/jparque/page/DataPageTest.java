package com.jparque.page;

import com.jparque.columnar.page.DataPage;
import com.jparque.columnar.page.Encoding;
import com.jparque.columnar.page.PageType;
import com.jparque.columnar.page.Statistics;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

import java.nio.ByteBuffer;

class DataPageTest {
    @Test
    void shouldWriteHeaderCorrectly() {
        // Create test data
        byte[] data = new byte[] { 1, 2, 3, 4 };
        ByteBuffer buffer = ByteBuffer.wrap(data);
        
        // Create statistics
        ByteBuffer minValue = ByteBuffer.wrap(new byte[] { 1 });
        ByteBuffer maxValue = ByteBuffer.wrap(new byte[] { 4 });
        Statistics stats = new Statistics(minValue, maxValue, 0, 4);

        // Create page
        DataPage page = new DataPage(
            buffer,
            4, // value count
            Encoding.PLAIN,
            Encoding.RLE,
            Encoding.BIT_PACKED,
            stats
        );

        // Write header
        ByteBuffer headerBuffer = ByteBuffer.allocate(page.getHeaderSize());
        page.writeHeader(headerBuffer);
        headerBuffer.flip();

        // Verify header contents
        assertThat(headerBuffer.get()).isEqualTo((byte) PageType.DATA_PAGE.getValue());
        assertThat(headerBuffer.getInt()).isEqualTo(4); // uncompressed size
        assertThat(headerBuffer.getInt()).isEqualTo(4); // compressed size
        assertThat(headerBuffer.getInt()).isEqualTo(4); // value count
        assertThat(headerBuffer.getInt()).isEqualTo(Encoding.PLAIN.getValue());
        assertThat(headerBuffer.getInt()).isEqualTo(Encoding.RLE.getValue());
        assertThat(headerBuffer.getInt()).isEqualTo(Encoding.BIT_PACKED.getValue());
        
        // Verify statistics
        assertThat(headerBuffer.getInt()).isEqualTo(1); // min value length
        assertThat(headerBuffer.get()).isEqualTo((byte) 1); // min value
        assertThat(headerBuffer.getInt()).isEqualTo(1); // max value length
        assertThat(headerBuffer.get()).isEqualTo((byte) 4); // max value
        assertThat(headerBuffer.getLong()).isEqualTo(0); // null count
        assertThat(headerBuffer.getLong()).isEqualTo(4); // distinct count
    }

    @Test
    void shouldHandleNullStatistics() {
        byte[] data = new byte[] { 1, 2, 3, 4 };
        ByteBuffer buffer = ByteBuffer.wrap(data);
        
        DataPage page = new DataPage(
            buffer,
            4,
            Encoding.PLAIN,
            Encoding.RLE,
            Encoding.BIT_PACKED,
            null // no statistics
        );

        ByteBuffer headerBuffer = ByteBuffer.allocate(page.getHeaderSize());
        page.writeHeader(headerBuffer);
        
        assertThat(headerBuffer.position()).isEqualTo(25); // header size without statistics
    }
}
