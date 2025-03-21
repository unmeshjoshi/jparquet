package com.jparque.btree.page;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Element represents an entry in a page's elements array.
 * Each element points to a key/value pair or key/page reference.
 */
public class Element {
    private final Page page;
    private final int offset;
    
    /**
     * Creates a new element reference at the given offset in the page.
     */
    public Element(Page page, int offset) {
        this.page = page;
        this.offset = offset;
    }
    
    /**
     * Returns the offset to the key/value data.
     */
    public int pos() {
        return getInt(0);
    }
    
    /**
     * Sets the offset to the key/value data.
     */
    public void setPos(int pos) {
        putInt(0, pos);
    }
    
    /**
     * Returns the element flags.
     */
    public int flags() {
        return getInt(4);
    }
    
    /**
     * Sets the element flags.
     */
    public void setFlags(int flags) {
        putInt(4, flags);
    }
    
    /**
     * Returns the size of the key.
     */
    public int keySize() {
        return getInt(8);
    }
    
    /**
     * Sets the size of the key.
     */
    public void setKeySize(int size) {
        putInt(8, size);
    }
    
    /**
     * Returns the size of the value (for leaf nodes) or the page ID (for branch nodes).
     */
    public int valueSize() {
        return getInt(12);
    }
    
    /**
     * Sets the size of the value or the page ID.
     */
    public void setValueSize(int size) {
        putInt(12, size);
    }
    
    /**
     * Returns the key data.
     */
    public byte[] key() {
        try {
            int p = pos();
            int ksize = keySize();
            
            // Validate position and size
            if (p < 0 || p >= page.data().length) {
                System.err.println("Invalid key position: " + p + ", Page size: " + page.data().length);
                return new byte[0]; // Return empty array if position is invalid
            }
            
            // Safety check to ensure we don't try to read beyond the page boundary
            if (ksize <= 0) {
                System.err.println("Empty or invalid key size: " + ksize);
                return new byte[0]; // Return empty array if no key
            }
            
            if (p + ksize > page.data().length) {
                System.err.println("Warning: Attempting to read key beyond page boundary.");
                System.err.println("Position: " + p + ", Size: " + ksize + ", Page size: " + page.data().length);
                // Limit to available data to prevent buffer overflow
                ksize = Math.max(0, Math.min(ksize, page.data().length - p));
            }
            
            byte[] key = new byte[ksize];
            ByteBuffer buffer = ByteBuffer.wrap(page.data());
            buffer.position(p);
            buffer.get(key);
            return key;
        } catch (Exception e) {
            System.err.println("Exception in Element.key(): " + e.getMessage());
            e.printStackTrace();
            return new byte[0]; // Return empty array on error
        }
    }
    
    /**
     * Returns the value data.
     * Note: If this element has overflow pages, the returned value
     * will be a serialized page ID (8 bytes) that should be handled by BPlusTree.
     */
    public byte[] value() {
        try {
            // In Page.putElement, values are placed BEFORE keys, not after
            // The key is at pos() and the value is at valuePos which is lower than pos
            // We need to find where the value actually is
            int keyPos = pos();
            int valueSize = valueSize();
            
            // In Page.putElement: valuePos = dataPos - value.length where dataPos is the key position
            int valuePos = keyPos - valueSize;
            
            // Validate position and size
            if (valuePos < 0 || valuePos >= page.data().length) {
                System.err.println("Invalid value position: " + valuePos + ", Page size: " + page.data().length);
                return new byte[0]; // Return empty array if position is invalid
            }
            
            // Safety check to ensure we don't try to read beyond the page boundary
            if (valueSize <= 0) {
                System.err.println("Empty or invalid value size: " + valueSize);
                return new byte[0]; // Return empty array if no value
            }
            
            if (valuePos + valueSize > page.data().length) {
                System.err.println("Warning: Attempting to read value beyond page boundary.");
                System.err.println("Value position: " + valuePos + ", Size: " + valueSize + ", Page size: " + page.data().length);
                // Limit to available data to prevent buffer overflow
                valueSize = Math.max(0, Math.min(valueSize, page.data().length - valuePos));
            }
            
            // Even if this has overflow, we still return the raw bytes here
            // (which will be a serialized page ID)
            // The caller is responsible for checking hasOverflow() and handling appropriately
            byte[] value = new byte[valueSize];
            ByteBuffer buffer = ByteBuffer.wrap(page.data());
            buffer.position(valuePos);
            buffer.get(value);
            return value;
        } catch (Exception e) {
            System.err.println("Exception in Element.value(): " + e.getMessage());
            e.printStackTrace();
            return new byte[0]; // Return empty array on error
        }
    }
    
    /**
     * Returns true if this element has overflow pages.
     */
    public boolean hasOverflow() {
        return (flags() & 0x1) != 0;
    }
    
    /**
     * Returns the overflow page ID for this element.
     * Only valid if hasOverflow() returns true.
     */
    public long overflowPageId() {
        if (!hasOverflow()) {
            throw new IllegalStateException("Element does not have overflow pages");
        }
        
        byte[] pageIdBytes = value();
        ByteBuffer buffer = ByteBuffer.wrap(pageIdBytes);
        return buffer.getLong();
    }
    
    /**
     * Sets the value data.
     * For simplicity, this implementation only supports overwriting values of the same size.
     * For different sizes, a more complex implementation would be needed.
     */
    public void setValue(byte[] value) {
        int keyPos = pos();
        int oldVsize = valueSize();
        
        // Value is located BEFORE the key in memory (per Page.putElement implementation)
        int valuePos = keyPos - oldVsize;
        
        // If new value is same size, just overwrite
        if (value.length == oldVsize) {
            ByteBuffer buffer = ByteBuffer.wrap(page.data());
            buffer.position(valuePos);
            buffer.put(value);
            return;
        }
        
        // For different sizes, we'd need to reorganize the page
        // This is a simplified implementation that doesn't handle this case properly
        throw new UnsupportedOperationException(
            "Updating values of different sizes is not supported in this implementation");
    }
    
    /**
     * Helper method to get an integer at the given offset.
     */
    private int getInt(int fieldOffset) {
        byte[] data = page.data();
        int position = offset + fieldOffset;
        
        // Boundary check
        if (position < 0 || position + 4 > data.length) {
            System.err.println("Warning: Element trying to access invalid offset: " + position);
            return 0; // Default to 0 for safety
        }
        
        ByteBuffer buffer = ByteBuffer.wrap(data);
        return buffer.getInt(position);
    }
    
    /**
     * Helper method to put an integer at the given offset.
     */
    private void putInt(int fieldOffset, int value) {
        byte[] data = page.data();
        int position = offset + fieldOffset;
        
        // Boundary check
        if (position < 0 || position + 4 > data.length) {
            System.err.println("Warning: Element trying to write to invalid offset: " + position);
            return; // Skip write for safety
        }
        
        ByteBuffer buffer = ByteBuffer.wrap(data);
        buffer.putInt(position, value);
    }
}
