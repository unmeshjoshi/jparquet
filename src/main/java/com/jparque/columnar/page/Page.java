package com.jparque.columnar.page;

import java.nio.ByteBuffer;
import com.jparque.columnar.page.PageType;

/**
 * Represents a page in a Parquet file.
 * A page is the smallest unit of storage in a Parquet file.
 */
public abstract class Page {
    private final PageType type;
    private final int uncompressedSize;
    private int compressedSize;
    private final ByteBuffer data;

    protected Page(PageType type, ByteBuffer data) {
        this.type = type;
        this.data = data;
        this.uncompressedSize = data.remaining();
        this.compressedSize = data.remaining();
    }

    public void setCompressedSize(int compressedSize) {
        this.compressedSize = compressedSize;
    }

    public PageType getType() {
        return type;
    }

    public int getUncompressedSize() {
        return uncompressedSize;
    }

    public int getCompressedSize() {
        return compressedSize;
    }

    public ByteBuffer getData() {
        return data.duplicate();
    }

    /**
     * Returns the total size of the page header in bytes.
     */
    public abstract int getHeaderSize();

    /**
     * Writes the page header to the given buffer.
     */
    public abstract void writeHeader(ByteBuffer buffer);
}
