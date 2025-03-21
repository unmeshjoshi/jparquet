package com.jparque.btree.page;

import com.jparque.btree.page.Page;
import com.jparque.btree.page.PageManager;
import com.jparque.btree.page.Element;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

public class PageManagerTest {
    
    @TempDir
    Path tempDir;
    
    private Path dbFile;
    private PageManager pageManager;
    private static final int PAGE_SIZE = 4096;
    
    @BeforeEach
    public void setUp() throws IOException {
        dbFile = tempDir.resolve("test.db");
        pageManager = new PageManager(dbFile, PAGE_SIZE);
    }
    
    @AfterEach
    public void tearDown() throws IOException {
        if (pageManager != null) {
            pageManager.close();
        }
    }
    
    @Test
    public void shouldAllocateNewPage() throws IOException {
        // Allocate a new page
        long pageId = pageManager.allocatePage();
        
        // Page IDs start at 1 (0 is reserved)
        assertEquals(1, pageId);
        
        // Allocate another page
        long pageId2 = pageManager.allocatePage();
        assertEquals(2, pageId2);
    }
    
    @Test
    public void shouldWriteAndReadPage() throws IOException {
        // Allocate a new page
        long pageId = pageManager.allocatePage();
        
        // Create a page with some data
        Page page = new Page(PAGE_SIZE);
        page.setId(pageId);
        
        // Add some data to the page
        byte[] key = "test-key".getBytes();
        byte[] value = "test-value".getBytes();
        page.putElement(key, value);
        
        // Write the page to disk
        pageManager.writePage(page);
        
        // Read the page back
        Page readPage = pageManager.readPage(pageId);
        
        // Verify the page was read correctly
        assertEquals(pageId, readPage.id());
        assertEquals(1, readPage.count());
        
        // Verify the data was read correctly
        Element elem = readPage.element(0);
        assertArrayEquals(key, elem.key());
        assertArrayEquals(value, elem.value());
    }
    
    @Test
    public void shouldCachePagesForFasterAccess() throws IOException {
        // Allocate a new page
        long pageId = pageManager.allocatePage();
        
        // Create a page with some data
        Page page = new Page(PAGE_SIZE);
        page.setId(pageId);
        page.putElement("test-key".getBytes(), "test-value".getBytes());
        
        // Write the page to disk
        pageManager.writePage(page);
        
        // Read the page multiple times (should use cache after first read)
        long startTime = System.nanoTime();
        Page readPage1 = pageManager.readPage(pageId);
        long firstReadTime = System.nanoTime() - startTime;
        
        startTime = System.nanoTime();
        Page readPage2 = pageManager.readPage(pageId);
        long secondReadTime = System.nanoTime() - startTime;
        
        // Second read should be faster (from cache)
        assertTrue(secondReadTime < firstReadTime);
        
        // Both reads should return the same data
        assertEquals(1, readPage1.count());
        assertEquals(1, readPage2.count());
        assertArrayEquals(readPage1.element(0).key(), readPage2.element(0).key());
        assertArrayEquals(readPage1.element(0).value(), readPage2.element(0).value());
    }
    
    @Test
    public void shouldPersistDataAcrossRestarts() throws IOException {
        // Allocate a new page
        long pageId = pageManager.allocatePage();
        
        // Create a page with some data
        Page page = new Page(PAGE_SIZE);
        page.setId(pageId);
        page.putElement("test-key".getBytes(), "test-value".getBytes());
        
        // Write the page to disk
        pageManager.writePage(page);
        pageManager.sync();
        pageManager.close();
        
        // Create a new PageManager (simulating a restart)
        pageManager = new PageManager(dbFile, PAGE_SIZE);
        
        // Read the page back
        Page readPage = pageManager.readPage(pageId);
        
        // Verify the page was read correctly
        assertEquals(pageId, readPage.id());
        assertEquals(1, readPage.count());
        
        // Verify the data was read correctly
        Element elem = readPage.element(0);
        assertArrayEquals("test-key".getBytes(), elem.key());
        assertArrayEquals("test-value".getBytes(), elem.value());
    }
}
