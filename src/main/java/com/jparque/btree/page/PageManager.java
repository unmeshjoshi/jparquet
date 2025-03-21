package com.jparque.btree.page;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;

/**
 * PageManager handles the allocation, reading, and writing of pages to disk.
 * It also maintains a simple cache of recently used pages.
 */
public class PageManager {
    private final FileChannel fileChannel;
    private final int pageSize;
    private long nextPageId = 1; // Reserve 0 for null/invalid page
    
    // Simple in-memory cache of pages
    private final Map<Long, Page> pageCache = new HashMap<>();
    
    // Maximum number of pages to keep in cache
    private final int maxCacheSize;
    
    /**
     * Creates a new PageManager with the specified file path and page size.
     */
    public PageManager(Path filePath, int pageSize) throws IOException {
        this.pageSize = pageSize;
        this.maxCacheSize = 1000; // Default cache size
        
        // Open or create the file
        this.fileChannel = FileChannel.open(filePath, 
            StandardOpenOption.READ, 
            StandardOpenOption.WRITE, 
            StandardOpenOption.CREATE);
        
        // Initialize the file if it's new
        if (fileChannel.size() == 0) {
            initializeFile();
        } else {
            // Read the next page ID from the file header
            ByteBuffer buffer = ByteBuffer.allocate(8);
            fileChannel.read(buffer, 0);
            buffer.flip();
            nextPageId = buffer.getLong();
        }
    }
    
    /**
     * Initializes a new file with a header page.
     */
    private void initializeFile() throws IOException {
        // Create a header page that stores metadata
        ByteBuffer headerBuffer = ByteBuffer.allocate(pageSize);
        
        // Write the next page ID (start at 1, as 0 is reserved)
        headerBuffer.putLong(nextPageId);
        
        // Fill the rest with zeros
        while (headerBuffer.hasRemaining()) {
            headerBuffer.put((byte) 0);
        }
        
        // Write the header page
        headerBuffer.flip();
        fileChannel.write(headerBuffer, 0);
        fileChannel.force(true);
    }
    
    /**
     * Allocates a new page and returns its ID.
     */
    public long allocatePage() throws IOException {
        long pageId = nextPageId++;
        
        // Update the next page ID in the file header
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(nextPageId);
        buffer.flip();
        fileChannel.write(buffer, 0);
        
        // Create a new empty page
        Page page = new Page(pageSize);
        page.setId(pageId);
        
        // Write the page to disk
        writePage(page);
        
        return pageId;
    }
    
    /**
     * Reads a page from disk by its ID.
     */
    public Page readPage(long pageId) throws IOException {
        // Check if the page is in the cache
        if (pageCache.containsKey(pageId)) {
            return pageCache.get(pageId);
        }
        
        // Calculate the offset in the file
        long offset = pageId * pageSize;
        
        // Read the page from disk
        ByteBuffer buffer = ByteBuffer.allocate(pageSize);
        fileChannel.read(buffer, offset);
        buffer.flip();
        
        // Create a new page from the buffer
        byte[] data = new byte[pageSize];
        buffer.get(data);
        Page page = new Page(data);
        
        // Add the page to the cache
        addToCache(pageId, page);
        
        return page;
    }
    
    /**
     * Writes a page to disk.
     */
    public void writePage(Page page) throws IOException {
        long pageId = page.id();
        
        // Calculate the offset in the file
        long offset = pageId * pageSize;
        
        // Write the page to disk
        ByteBuffer buffer = ByteBuffer.wrap(page.data());
        fileChannel.write(buffer, offset);
        
        // Update the cache
        addToCache(pageId, page);
    }
    
    /**
     * Adds a page to the cache, evicting old pages if necessary.
     */
    private void addToCache(long pageId, Page page) {
        // If the cache is full, remove the oldest entry
        if (pageCache.size() >= maxCacheSize) {
            // Simple eviction strategy: remove a random entry
            // In a real implementation, this would use LRU or similar
            if (!pageCache.isEmpty()) {
                pageCache.remove(pageCache.keySet().iterator().next());
            }
        }
        
        // Add the page to the cache
        pageCache.put(pageId, page);
    }
    
    /**
     * Forces all changes to disk.
     */
    public void sync() throws IOException {
        fileChannel.force(true);
    }
    
    /**
     * Closes the file channel.
     */
    public void close() throws IOException {
        fileChannel.close();
    }
    
    /**
     * Returns the configured page size.
     */
    public int getPageSize() {
        return pageSize;
    }
}
