package com.jparque.columnar.page;

import java.nio.ByteBuffer;
import com.jparque.columnar.page.Page;
import com.jparque.columnar.page.Encoding;
import com.jparque.columnar.page.Statistics;

/**
 * Represents a data page in a Parquet file.
 * Data pages contain the actual column data in compressed or uncompressed form.
 */
public class DataPage extends Page {
    private final int valueCount;
    private final Encoding encoding;
    private final Encoding definitionLevelEncoding;
    private final Encoding repetitionLevelEncoding;
    private final Statistics statistics;

    public DataPage(ByteBuffer data, 
                   int valueCount,
                   Encoding encoding,
                   Encoding definitionLevelEncoding,
                   Encoding repetitionLevelEncoding,
                   Statistics statistics) {
        super(PageType.DATA_PAGE, data);
        this.valueCount = valueCount;
        this.encoding = encoding;
        this.definitionLevelEncoding = definitionLevelEncoding;
        this.repetitionLevelEncoding = repetitionLevelEncoding;
        this.statistics = statistics;
    }

    @Override
    public int getHeaderSize() {
        // Size calculation:
        // - 1 byte for page type
        // - 4 bytes for uncompressed size
        // - 4 bytes for compressed size
        // - 4 bytes for value count
        // - 4 bytes for encoding
        // - 4 bytes for definition level encoding
        // - 4 bytes for repetition level encoding
        // - Statistics size (if present)
        return 25 + (statistics != null ? statistics.getSize() : 0);
    }

    @Override
    public void writeHeader(ByteBuffer buffer) {
        buffer.put((byte) getType().getValue());
        buffer.putInt(getUncompressedSize());
        buffer.putInt(getCompressedSize());
        buffer.putInt(valueCount);
        buffer.putInt(encoding.getValue());
        buffer.putInt(definitionLevelEncoding.getValue());
        buffer.putInt(repetitionLevelEncoding.getValue());
        if (statistics != null) {
            statistics.write(buffer);
        }
    }

    public int getValueCount() {
        return valueCount;
    }

    public Encoding getEncoding() {
        return encoding;
    }

    public Encoding getDefinitionLevelEncoding() {
        return definitionLevelEncoding;
    }

    public Encoding getRepetitionLevelEncoding() {
        return repetitionLevelEncoding;
    }

    public Statistics getStatistics() {
        return statistics;
    }
}
