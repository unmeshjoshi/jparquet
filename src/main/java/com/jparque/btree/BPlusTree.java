package com.jparque.btree;

import com.jparque.btree.page.Page;
import com.jparque.btree.page.PageManager;
import com.jparque.btree.page.Element;
import com.jparque.storage.Record;
import com.jparque.storage.StorageEngine;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.BufferUnderflowException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;

/**
 * B+Tree storage implementation.
 * Optimized for OLTP workloads with efficient point queries
 * and range scans. Maintains data in sorted order.
 */
public class BPlusTree implements StorageEngine {
    // Page size in bytes (default 4KB)
    private static final int DEFAULT_PAGE_SIZE = 4096;
    
    // B+Tree degree (max children per node)
    private final int degree;
    
    // Page manager for disk storage
    private final PageManager pageManager;
    
    // Root page ID
    private long rootPageId;
    
    // Serializer/deserializer for values
    private final ValueSerializer valueSerializer;
    
    /**
     * Creates a new B+Tree with a default configuration
     *
     * @param filePath Path to the database file
     * @throws IOException If an I/O error occurs
     */
    public BPlusTree(Path filePath) throws IOException {
        this(filePath, DEFAULT_PAGE_SIZE);
    }
    
    /**
     * Creates a new B+Tree with a custom page size
     *
     * @param filePath Path to the database file
     * @param pageSize Size of each page in bytes
     * @throws IOException If an I/O error occurs
     */
    public BPlusTree(Path filePath, int pageSize) throws IOException {
        this.pageManager = new PageManager(filePath, pageSize);
        this.valueSerializer = new ValueSerializer();
        
        // Calculate degree based on page size
        // For simplicity, we'll use a fixed calculation
        // In a real implementation, this would be more sophisticated
        this.degree = (pageSize - Page.PAGE_HEADER_SIZE) / (Page.ELEM_SIZE * 2);
        
        // Initialize the tree if it doesn't exist
        try {
            initializeTree();
        } catch (Exception e) {
            System.err.println("Error initializing tree: " + e.getMessage());
            throw new IOException("Failed to initialize B+Tree", e);
        }
    }
    
    /**
     * Initializes the B+Tree structure if it doesn't exist
     */
    private void initializeTree() throws IOException {
        try {
            // Check if root page exists and is valid
            Page rootPage = pageManager.readPage(rootPageId);
            int flags = rootPage.flags();
            if (flags != Page.FLAG_LEAF && flags != Page.FLAG_BRANCH) {
                System.err.println("Invalid root page flags: " + flags + ", recreating root");
                createRootPage();
            }
            // Root exists and is valid, nothing more to do
        } catch (IOException e) {
            // Root doesn't exist or is invalid, create it
            System.err.println("Creating new root page: " + e.getMessage());
            createRootPage();
        }
    }
    
    /**
     * Creates a new root page as a leaf node
     */
    private void createRootPage() throws IOException {
        // Allocate root page
        rootPageId = pageManager.allocatePage();
        Page rootPage = pageManager.readPage(rootPageId);
        rootPage.setFlags(Page.FLAG_LEAF); // Start with a leaf node as root
        pageManager.writePage(rootPage);
        System.out.println("Created new root page: " + rootPageId);
    }
    
    @Override
    public void write(byte[] key, Map<String, Object> value) throws IOException {
        // Serialize the value to bytes
        byte[] valueBytes = valueSerializer.serialize(value);
        
        // Start at the root page
        long currentPageId = rootPageId;
        Page currentPage = pageManager.readPage(currentPageId);
        
        // Keep track of path down the tree (for splitting)
        List<Long> path = new ArrayList<>();
        
        // Search for the leaf node where this key belongs
        while (currentPage.isBranch()) {
            path.add(currentPageId);
            
            // Find the next page to navigate to
            int index = findChildIndex(currentPage, key);
            
            // Make sure index is valid
            int count = currentPage.count();
            if (count == 0) {
                System.err.println("Warning: Branch page " + currentPageId + " has no elements");
                break; // Exit the loop, we're at a leaf node
            }
            
            if (index < 0 || index >= count) {
                System.err.println("Warning: Invalid child index " + index + " for count " + count);
                index = Math.min(Math.max(0, index), count - 1); // Clamp to valid range
            }
            
            Element element = currentPage.element(index);
            
            // The value of a branch node element is the page ID of the child
            ByteBuffer buffer = ByteBuffer.wrap(element.value());
            currentPageId = buffer.getLong();
            currentPage = pageManager.readPage(currentPageId);
        }
        
        // Now at leaf node, check if the key already exists
        int count = currentPage.count();
        for (int i = 0; i < count; i++) {
            Element element = currentPage.element(i);
            if (Arrays.equals(key, element.key())) {
                // Key exists - check if previous value had overflow pages
                if (element.hasOverflow()) {
                    // Free the old overflow pages
                    freeOverflowPages(element.overflowPageId());
                }
                break;
            }
        }
        
        // Calculate the maximum inline value size
        int maxInlineSize = calculateMaxInlineSize(currentPage, key);
        boolean success = false;
        
        // Determine if we need overflow pages
        if (valueBytes.length > maxInlineSize) {
            // Value too large, use overflow pages
            success = insertWithOverflow(currentPage, key, valueBytes);
        } else {
            // Value fits inline
            success = insertIntoLeaf(currentPage, key, valueBytes);
        }
        
        // If insertion succeeded, write the page back
        if (success) {
            pageManager.writePage(currentPage);
            return;
        }
        
        // Need to split the leaf node
        splitLeafNode(currentPage, key, valueBytes, path);
    }
    
    /**
     * Calculate the maximum inline value size that can fit in a leaf
     */
    private int calculateMaxInlineSize(Page leaf, byte[] key) {
        // Get available free space
        int freeSpace = leaf.freeSpace();
        
        // Account for the element header and key
        int overhead = 0;
        
        // If we're adding a completely new key, account for the element header
        boolean keyExists = false;
        int count = leaf.count();
        for (int i = 0; i < count; i++) {
            Element element = leaf.element(i);
            if (Arrays.equals(key, element.key())) {
                keyExists = true;
                break;
            }
        }
        
        if (!keyExists) {
            overhead += Page.ELEM_SIZE + key.length;
        }
        
        // Add some buffer space to be safe
        overhead += 32; // Safety margin
        
        // Calculate max size
        int maxSize = freeSpace - overhead;
        if (maxSize < 0) maxSize = 0;
        
        return maxSize;
    }
    
    /**
     * Inserts a key and large value using overflow pages
     */
    private boolean insertWithOverflow(Page leaf, byte[] key, byte[] value) throws IOException {
        try {
            // Create overflow pages for the value
            long overflowPageId = createOverflowPages(value);
            
            // Create a reference to the overflow pages (8-byte long ID)
            ByteBuffer buffer = ByteBuffer.allocate(8);
            buffer.putLong(overflowPageId);
            buffer.flip(); // Reset position before getting array
            byte[] overflowRef = buffer.array();
            
            // Try to insert with overflow flag set to true
            boolean success = leaf.putElement(key, overflowRef, true);
            
            if (!success) {
                // If insertion failed, clean up the overflow pages
                freeOverflowPages(overflowPageId);
            }
            
            return success;
        } catch (IOException e) {
            // Log and rethrow
            System.err.println("Error creating overflow pages: " + e.getMessage());
            throw e;
        }
    }
    
    // Special flag for overflow pages - different from leaf/branch flags
    private static final int OVERFLOW_FLAG = 0x10;
    
    /**
     * Creates overflow pages to store a large value
     */
    private long createOverflowPages(byte[] value) throws IOException {
        // Calculate how many pages we need
        int pageSize = pageManager.getPageSize();
        int dataPerPage = pageSize - Page.PAGE_HEADER_SIZE;
        int pagesNeeded = (value.length + dataPerPage - 1) / dataPerPage;
        
        if (pagesNeeded <= 0) {
            pagesNeeded = 1; // Minimum one page
        }
        
        // Create the chain of overflow pages
        long firstPageId = -1;
        long prevPageId = -1;
        
        for (int i = 0; i < pagesNeeded; i++) {
            // Allocate a new page
            long currentPageId = pageManager.allocatePage();
            Page page = pageManager.readPage(currentPageId);
            
            // Set page type to overflow
            page.setFlags(OVERFLOW_FLAG);
            
            // If this is the first page, save its ID
            if (firstPageId == -1) {
                firstPageId = currentPageId;
            }
            
            // Link from previous page if not the first page
            if (prevPageId != -1) {
                Page prevPage = pageManager.readPage(prevPageId);
                prevPage.setOverflow(currentPageId);
                pageManager.writePage(prevPage);
            }
            
            // Last page has no overflow
            if (i == pagesNeeded - 1) {
                page.setOverflow(0); // End of chain
            }
            
            // Calculate how much data to copy to this page
            int offset = i * dataPerPage;
            int length = Math.min(dataPerPage, value.length - offset);
            
            // Store the data length for easy retrieval
            page.setCount(length);
            
            // Copy data to the page
            ByteBuffer buffer = ByteBuffer.wrap(page.data());
            buffer.position(Page.PAGE_HEADER_SIZE);
            buffer.put(value, offset, length);
            
            // Write the page
            pageManager.writePage(page);
            
            // Update for next iteration
            prevPageId = currentPageId;
        }
        
        return firstPageId;
    }
    
    /**
     * Frees overflow pages starting from the given page ID
     */
    private void freeOverflowPages(long pageId) throws IOException {
        while (pageId != 0) {
            // Read the page
            Page page = pageManager.readPage(pageId);
            
            // Get the next page ID
            long nextPageId = page.overflow();
            
            // Free this page (in a real implementation, we would add it to the freelist)
            // For now, we'll just mark it as free by changing its flags
            page.setFlags(Page.FLAG_FREELIST);
            pageManager.writePage(page);
            
            // Move to the next page
            pageId = nextPageId;
        }
    }
    
    /**
     * Finds the index of the child page that should contain the given key
     */
    private int findChildIndex(Page page, byte[] key) {
        // Binary search to find the right child
        int count = page.count();
        
        // Edge case: if this is an empty branch (shouldn't happen), return 0
        if (count == 0) {
            System.err.println("Warning: Empty branch page encountered");
            return 0;
        }

        // First check if key is less than the first element
        Element firstElem = page.element(0);
        if (firstElem == null) {
            System.err.println("Warning: Null first element in branch page");
            return 0;
        }
        
        byte[] firstKey = firstElem.key();
        if (firstKey == null) {
            System.err.println("Warning: Null first key in branch page");
            return 0;
        }
        
        if (compareKeys(key, firstKey) < 0) {
            // Key is smaller than first key, return the first child
            return 0;
        }
        
        // Check if key is greater than or equal to the last element
        Element lastElem = page.element(count - 1);
        if (lastElem == null) {
            System.err.println("Warning: Null last element in branch page");
            return count - 1;
        }
        
        byte[] lastKey = lastElem.key();
        if (lastKey == null) {
            System.err.println("Warning: Null last key in branch page");
            return count - 1;
        }
        
        if (compareKeys(key, lastKey) >= 0) {
            // Key is greater than or equal to last key, return the last child
            return count - 1;
        }
        
        // Search for the first key greater than the target
        int low = 0;
        int high = count - 1;
        
        // Debug output
        System.out.println("Searching branch with " + count + " keys for key: " + new String(key));
        
        while (low <= high) {
            int mid = (low + high) >>> 1;
            Element midElem = page.element(mid);
            if (midElem == null) {
                System.err.println("Warning: Null element at index " + mid);
                return mid > 0 ? mid - 1 : 0; // Return previous index if possible
            }
            
            byte[] midKey = midElem.key();
            if (midKey == null) {
                System.err.println("Warning: Null key at index " + mid);
                return mid > 0 ? mid - 1 : 0; // Return previous index if possible
            }
            
            int cmp = compareKeys(key, midKey);
            System.out.println("  Compare key: '" + new String(key) + "' to '" + new String(midKey) + "' = " + cmp);
            
            if (cmp < 0) {
                high = mid - 1;
            } else if (cmp > 0) {
                low = mid + 1;
            } else {
                // Exact match, use this child
                System.out.println("  Exact match at index " + mid);
                return mid;
            }
        }
        
        // The key is between low-1 and low
        // Use the child at low-1 because that's the last key less than our search key
        int result = Math.max(0, low - 1);
        System.out.println("  No exact match, using index " + result);
        return result;
    }
    
    /**
     * Compares two keys lexicographically
     */
    private int compareKeys(byte[] a, byte[] b) {
        int len = Math.min(a.length, b.length);
        for (int i = 0; i < len; i++) {
            int diff = (a[i] & 0xFF) - (b[i] & 0xFF);
            if (diff != 0) {
                return diff;
            }
        }
        return a.length - b.length;
    }
    
    /**
     * Attempts to insert a key-value pair into a leaf node
     * @return true if successful, false if the node is full
     */
    private boolean insertIntoLeaf(Page leaf, byte[] key, byte[] value) {
        return leaf.putElement(key, value);
    }
    
    /**
     * Splits a leaf node when it's full
     */
    private void splitLeafNode(Page leaf, byte[] key, byte[] value, List<Long> path) throws IOException {
        // Create a new page for the right half
        long newPageId = pageManager.allocatePage();
        Page newLeaf = pageManager.readPage(newPageId);
        newLeaf.setFlags(Page.FLAG_LEAF);
        
        // For simplicity, we'll use the same approach as leaf splitting
        int splitPoint = degree / 2;
        
        // Find the position where our new key should be inserted
        int insertPos = findInsertPosition(leaf, key);
        
        // Distribute elements between the two pages
        distributeLeafElements(leaf, newLeaf, splitPoint, key, value, insertPos);
        
        // Get the first key of the new leaf for the parent
        Element firstElement = newLeaf.element(0);
        byte[] firstKey = firstElement.key();
        
        // Update parent or create new root
        if (path.isEmpty()) {
            // Current node is the root, create a new root
            createNewRoot(leaf.id(), newPageId, firstKey);
        } else {
            // Update the parent
            updateParentAfterSplit(path, firstKey, newPageId);
        }
        
        // Write both pages
        pageManager.writePage(leaf);
        pageManager.writePage(newLeaf);
    }
    
    /**
     * Finds the position where a key should be inserted in a leaf node
     */
    private int findInsertPosition(Page leaf, byte[] key) {
        int count = leaf.count();
        
        // Edge case: empty leaf
        if (count == 0) {
            return 0;
        }
        
        // Binary search to find insertion point
        int low = 0;
        int high = count - 1;
        
        while (low <= high) {
            int mid = (low + high) >>> 1;
            Element midElem = leaf.element(mid);
            int cmp = compareKeys(key, midElem.key());
            
            if (cmp < 0) {
                high = mid - 1;
            } else if (cmp > 0) {
                low = mid + 1;
            } else {
                // Exact match, update at this position
                return mid;
            }
        }
        
        return low;
    }
    
    /**
     * Distributes elements between two leaf pages during a split
     */
    private void distributeLeafElements(Page leftLeaf, Page rightLeaf, int splitPoint, 
                                       byte[] newKey, byte[] newValue, int insertPos) {
        // This is a simplified implementation
        // In a real implementation, we would handle this more efficiently
        
        // Extract all elements, including the new one
        int count = leftLeaf.count();
        List<byte[]> keys = new ArrayList<>(count + 1);
        List<byte[]> values = new ArrayList<>(count + 1);
        
        // Collect all keys and values
        for (int i = 0; i < count; i++) {
            Element elem = leftLeaf.element(i);
            keys.add(elem.key());
            values.add(elem.value());
        }
        
        // Insert the new key/value at the correct position
        if (insertPos <= keys.size()) {
            keys.add(insertPos, newKey);
            values.add(insertPos, newValue);
        } else {
            // Handle case where insertPos is beyond list boundaries
            System.err.println("Warning: Insert position " + insertPos + " exceeds list size " + keys.size() + ". Appending at the end.");
            keys.add(newKey);
            values.add(newValue);
        }
        
        // Adjust splitPoint if needed to prevent index out of bounds
        int totalElements = keys.size();
        if (splitPoint >= totalElements) {
            splitPoint = totalElements / 2;
            System.err.println("Adjusted splitPoint from exceeding total elements to: " + splitPoint);
        }
        
        // Clear the left leaf
        leftLeaf.setCount(0);
        
        // Redistribute to left leaf
        for (int i = 0; i < splitPoint && i < totalElements; i++) {
            leftLeaf.putElement(keys.get(i), values.get(i));
        }
        
        // Redistribute to right leaf
        for (int i = splitPoint; i < totalElements; i++) {
            rightLeaf.putElement(keys.get(i), values.get(i));
        }
    }
    
    /**
     * Creates a new root node when splitting the current root
     */
    private void createNewRoot(long leftPageId, long rightPageId, byte[] splitKey) throws IOException {
        // Allocate a new page for the root
        long newRootId = pageManager.allocatePage();
        Page newRoot = pageManager.readPage(newRootId);
        newRoot.setFlags(Page.FLAG_BRANCH);
        
        // Add entries for the two children
        // For the left child, we'll use a copy of the split key (or a minimum key)
        byte[] leftKey = new byte[splitKey.length];
        System.arraycopy(splitKey, 0, leftKey, 0, splitKey.length);
        // Decrement the last byte to make it less than the split key
        if (leftKey.length > 0) {
            leftKey[leftKey.length - 1]--;
        }
        
        // Add pointers to children
        ByteBuffer leftBuffer = ByteBuffer.allocate(8);
        leftBuffer.putLong(leftPageId);
        leftBuffer.flip(); // Reset position before getting array
        newRoot.putElement(leftKey, leftBuffer.array());
        
        ByteBuffer rightBuffer = ByteBuffer.allocate(8);
        rightBuffer.putLong(rightPageId);
        rightBuffer.flip(); // Reset position before getting array
        newRoot.putElement(splitKey, rightBuffer.array());
        
        // Update root page ID
        rootPageId = newRootId;
        
        // Write the new root
        pageManager.writePage(newRoot);
    }
    
    /**
     * Updates a parent node after a child splits
     */
    private void updateParentAfterSplit(List<Long> path, byte[] splitKey, long newPageId) throws IOException {
        // Get the parent page
        long parentId = path.remove(path.size() - 1);
        Page parent = pageManager.readPage(parentId);
        
        // Create a value containing the page ID
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(newPageId);
        buffer.flip(); // Reset position before getting array
        byte[] value = buffer.array();
        
        // Try to insert the new key/page pointer
        boolean success = parent.putElement(splitKey, value);
        
        if (success) {
            // If insertion succeeded, write the page back
            pageManager.writePage(parent);
            return;
        }
        
        // Need to split the branch node
        splitBranchNode(parent, splitKey, value, path);
    }
    
    /**
     * Splits a branch node when it's full
     */
    private void splitBranchNode(Page branch, byte[] key, byte[] value, List<Long> path) throws IOException {
        // Create a new page for the right half
        long newPageId = pageManager.allocatePage();
        Page newBranch = pageManager.readPage(newPageId);
        newBranch.setFlags(Page.FLAG_BRANCH);
        
        // For simplicity, we'll use the same approach as leaf splitting
        int splitPoint = degree / 2;
        
        // Find the position where our new key should be inserted
        int insertPos = findInsertPosition(branch, key);
        
        // Distribute elements between the two pages
        byte[] promotedKey = distributeBranchElements(branch, newBranch, splitPoint, key, value, insertPos);
        
        // Update parent or create new root
        if (path.isEmpty()) {
            // Current node is the root, create a new root
            createNewRoot(branch.id(), newPageId, promotedKey);
        } else {
            // Update the parent
            updateParentAfterSplit(path, promotedKey, newPageId);
        }
        
        // Write both pages
        pageManager.writePage(branch);
        pageManager.writePage(newBranch);
    }
    
    /**
     * Distributes elements between two branch pages during a split
     * @return The key to be promoted to the parent
     */
    private byte[] distributeBranchElements(Page leftBranch, Page rightBranch, int splitPoint, 
                                           byte[] newKey, byte[] newValue, int insertPos) {
        // Extract all elements, including the new one
        int count = leftBranch.count();
        List<byte[]> keys = new ArrayList<>(count + 1);
        List<byte[]> values = new ArrayList<>(count + 1);
        
        // Collect all keys and values
        for (int i = 0; i < count; i++) {
            Element elem = leftBranch.element(i);
            keys.add(elem.key());
            values.add(elem.value());
        }
        
        // Insert the new key/value at the correct position
        keys.add(insertPos, newKey);
        values.add(insertPos, newValue);
        
        // Clear the left branch
        leftBranch.setCount(0);
        
        // Redistribute to left branch (keep splitPoint keys)
        for (int i = 0; i < splitPoint; i++) {
            leftBranch.putElement(keys.get(i), values.get(i));
        }
        
        // The middle key will be promoted to the parent
        byte[] promotedKey = keys.get(splitPoint);
        
        // Redistribute to right branch (skip the promoted key)
        for (int i = splitPoint + 1; i < keys.size(); i++) {
            rightBranch.putElement(keys.get(i), values.get(i));
        }
        
        return promotedKey;
    }
    
    @Override
    public Optional<Map<String, Object>> read(byte[] key) throws IOException {
        try {
            // Basic validation
            if (key == null || key.length == 0) {
                System.err.println("Warning: Attempted to read with null or empty key");
                return Optional.empty();
            }
            
            // Start search at the root
            long currentPageId = rootPageId;
            System.out.println("Reading from root page: " + currentPageId);
            
            // Validate root page ID
            if (currentPageId <= 0) {
                System.err.println("Invalid root page ID: " + currentPageId);
                throw new IOException("Invalid root page ID: " + currentPageId);
            }
            
            try {
                Page currentPage = pageManager.readPage(currentPageId);
                
                // Validate that we got a page
                if (currentPage == null) {
                    System.err.println("Failed to read root page: " + currentPageId);
                    throw new IOException("Failed to read root page: " + currentPageId);
                }
                
                // Traverse down to the leaf node
                while (currentPage.isBranch()) {
                    // Find the child page that should contain this key
                    int count = currentPage.count();
                    if (count == 0) {
                        System.err.println("Branch page is empty: " + currentPageId);
                        throw new IOException("Branch page is empty: " + currentPageId);
                    }
                    
                    int index = findChildIndex(currentPage, key);
                    if (index < 0 || index >= count) {
                        System.err.println("Invalid child index: " + index + ", count: " + count);
                        throw new IOException("Invalid child index");
                    }
                    
                    Element element = currentPage.element(index);
                    if (element == null) {
                        System.err.println("Null element at index: " + index);
                        throw new IOException("Null element in branch page");
                    }
                    
                    // Extract page ID from the value
                    byte[] valueBytes = element.value();
                    System.out.println("Element value length: " + (valueBytes != null ? valueBytes.length : "null"));
                    
                    // Check if value is valid
                    if (valueBytes == null || valueBytes.length < 8) {
                        System.err.println("Warning: Invalid value length for page ID: " + 
                            (valueBytes != null ? valueBytes.length : "null") + 
                            " - This usually means the tree is corrupt or was not initialized properly");
                        throw new IOException("Invalid value length for branch pointer");
                    }
                    
                    try {
                        ByteBuffer buffer = ByteBuffer.wrap(valueBytes);
                        buffer.rewind(); // Reset the buffer position
                        currentPageId = buffer.getLong();
                        System.out.println("Following branch to page: " + currentPageId);
                        
                        if (currentPageId <= 0) {
                            System.err.println("Invalid page ID in branch: " + currentPageId);
                            throw new IOException("Invalid page ID in branch node");
                        }
                        
                        currentPage = pageManager.readPage(currentPageId);
                        
                        // Validate the page type
                        if (currentPage == null) {
                            throw new IOException("Failed to read page: " + currentPageId);
                        }
                    } catch (BufferUnderflowException e) {
                        System.err.println("Buffer underflow reading branch pointer: " + e.getMessage());
                        throw new IOException("Failed to read branch pointer", e);
                    }
                }
                
                // Now at leaf node, search for the key
                int count = currentPage.count();
                for (int i = 0; i < count; i++) {
                    Element element = currentPage.element(i);
                    if (element == null) {
                        System.err.println("Null element at index: " + i + " in leaf page");
                        continue;
                    }
                    
                    byte[] elementKey = element.key();
                    if (elementKey == null) {
                        System.err.println("Null key in element at index: " + i);
                        continue;
                    }
                    
                    if (Arrays.equals(key, elementKey)) {
                        // Found the key, check if it has overflow pages
                        byte[] valueBytes;
                        if (element.hasOverflow()) {
                            try {
                                // Read from overflow pages
                                byte[] overflowBytes = element.value();
                                System.out.println("Overflow element value length: " + 
                                    (overflowBytes != null ? overflowBytes.length : "null"));
                                
                                if (overflowBytes == null || overflowBytes.length < 8) {
                                    System.err.println("Invalid overflow reference length: " + 
                                        (overflowBytes != null ? overflowBytes.length : "null"));
                                    throw new IOException("Invalid overflow reference");
                                }
                                
                                ByteBuffer buffer = ByteBuffer.wrap(overflowBytes);
                                buffer.rewind(); // Reset the buffer position
                                long overflowPageId = buffer.getLong();
                                
                                if (overflowPageId <= 0) {
                                    System.err.println("Invalid overflow page ID: " + overflowPageId);
                                    throw new IOException("Invalid overflow page ID");
                                }
                                
                                System.out.println("Reading from overflow page: " + overflowPageId);
                                valueBytes = readFromOverflowPages(overflowPageId);
                                
                                if (valueBytes == null) {
                                    throw new IOException("Failed to read from overflow pages");
                                }
                            } catch (BufferUnderflowException e) {
                                System.err.println("Buffer underflow reading overflow reference: " + e.getMessage());
                                throw new IOException("Failed to read overflow reference", e);
                            }
                        } else {
                            // Regular inline value
                            valueBytes = element.value();
                        }
                        
                        if (valueBytes == null || valueBytes.length == 0) {
                            System.err.println("Warning: Empty or null value bytes for key");
                            return Optional.empty();
                        }
                        
                        // Deserialize the value
                        Map<String, Object> value = valueSerializer.deserialize(valueBytes);
                        if (value == null || value.isEmpty()) {
                            System.err.println("Warning: Deserialization returned null or empty map");
                        }
                        return Optional.of(value);
                    }
                }
                
                // Key not found
                return Optional.empty();
            } catch (Exception e) {
                System.err.println("Unexpected exception during read: " + e.getMessage());
                e.printStackTrace();
                throw new IOException("Failed to read value", e);
            }
        } catch (Exception e) {
            System.err.println("Exception in read: " + e.getMessage());
            e.printStackTrace();
            throw new IOException("Read operation failed", e);
        }
    }
    
    /**
     * Reads value data from overflow pages
     */
    private byte[] readFromOverflowPages(long startPageId) throws IOException {
        if (startPageId <= 0) {
            throw new IOException("Invalid overflow page ID: " + startPageId);
        }
        
        // Track visited pages to detect cycles
        Set<Long> visitedPages = new HashSet<>();
        
        // First, calculate the total size of the value
        int totalSize = 0;
        long pageId = startPageId;
        
        try {
            while (pageId != 0 && !visitedPages.contains(pageId)) {
                visitedPages.add(pageId);
                
                // Safety check for overflow chain size
                if (visitedPages.size() > 2000) { // Increased limit to handle larger values
                    System.err.println("Warning: Extremely large overflow chain detected: " + visitedPages.size() + " pages");
                    throw new IOException("Overflow chain too large");
                }
                
                Page page = pageManager.readPage(pageId);
                if (page == null) {
                    throw new IOException("Failed to read overflow page: " + pageId);
                }
                
                // Validate this is an overflow page
                if (page.isLeaf() || page.isBranch()) {
                    throw new IOException("Invalid page type in overflow chain: expected overflow, got " + 
                                        (page.isLeaf() ? "leaf" : "branch"));
                }
                
                int count = page.count();
                // Validate count is reasonable
                if (count < 0 || count > pageManager.getPageSize() - Page.PAGE_HEADER_SIZE) {
                    throw new IOException("Invalid data size in overflow page: " + count);
                }
                
                totalSize += count; // We stored the data length in count
                pageId = page.overflow();
            }
        } catch (IOException e) {
            System.err.println("Error calculating overflow data size: " + e.getMessage());
            throw e;
        }
        
        // Check for cycle in the chain
        if (visitedPages.contains(pageId) && pageId != 0) {
            throw new IOException("Cycle detected in overflow chain");
        }
        
        // Cap the total size for safety
        if (totalSize > 50 * 1024 * 1024) { // 50MB cap
            System.err.println("Warning: Extremely large value detected: " + totalSize + " bytes");
            throw new IOException("Value size exceeds safety limit: " + totalSize + " bytes");
        }
        
        // Allocate a buffer for the entire value
        byte[] result;
        try {
            result = new byte[totalSize];
        } catch (OutOfMemoryError e) {
            System.err.println("Failed to allocate buffer for large value: " + totalSize + " bytes");
            throw new IOException("Cannot allocate buffer for large value: " + e.getMessage(), e);
        }
        
        // Reset for second pass
        int offset = 0;
        pageId = startPageId;
        visitedPages.clear();
        
        try {
            // Read data from each overflow page
            while (pageId != 0 && !visitedPages.contains(pageId)) {
                visitedPages.add(pageId);
                
                Page page = pageManager.readPage(pageId);
                if (page == null) {
                    throw new IOException("Failed to read overflow page: " + pageId);
                }
                
                int length = page.count();
                
                // Validate count again
                if (length < 0 || length > page.data().length - Page.PAGE_HEADER_SIZE) {
                    throw new IOException("Invalid data length in overflow page: " + length);
                }
                
                // Validate offset is within bounds
                if (offset + length > result.length) {
                    throw new IOException("Buffer overflow while reading overflow pages: offset=" + 
                                         offset + ", length=" + length + ", buffer size=" + result.length);
                }
                
                // Copy data from this page
                try {
                    ByteBuffer buffer = ByteBuffer.wrap(page.data());
                    buffer.position(Page.PAGE_HEADER_SIZE);
                    buffer.get(result, offset, length);
                } catch (Exception e) {
                    throw new IOException("Error reading data from overflow page: " + e.getMessage(), e);
                }
                
                // Move to next page
                offset += length;
                pageId = page.overflow();
            }
            
            if (offset != totalSize) {
                System.err.println("Warning: Read size " + offset + " does not match expected size " + totalSize);
            }
            
            return result;
        } catch (IOException e) {
            System.err.println("Error reading from overflow pages: " + e.getMessage());
            throw e;
        }
    }
    
    @Override
    public List<Record> scan(byte[] startKey, byte[] endKey, List<String> columns) throws IOException {
        List<Record> results = new ArrayList<>();
        
        try {
            // Basic validation
            if (startKey == null || startKey.length == 0) {
                System.err.println("Warning: startKey is null or empty, using empty array");
                startKey = new byte[0];
            }
            
            System.out.println("Scanning from '" + new String(startKey) + "' to '" + 
                             (endKey != null ? new String(endKey) : "end") + "'");
            
            // Start at the root
            long currentPageId = rootPageId;
            System.out.println("Starting scan at root page: " + currentPageId);
            Page currentPage = pageManager.readPage(currentPageId);
            
            // Find the leaf node containing the start key
            while (currentPage.isBranch()) {
                int index = findChildIndex(currentPage, startKey);
                System.out.println("Following branch at index " + index + " for start key");
                Element element = currentPage.element(index);
                
                if (element == null) {
                    System.err.println("Error: null element at index " + index);
                    return results; // Return empty results
                }
                
                byte[] valueBytes = element.value();
                if (valueBytes == null || valueBytes.length < 8) {
                    System.err.println("Error: invalid value bytes for branch pointer");
                    return results; // Return empty results
                }
                
                try {
                    ByteBuffer buffer = ByteBuffer.wrap(valueBytes);
                    buffer.rewind(); // Reset the buffer position
                    currentPageId = buffer.getLong();
                    System.out.println("Following to page: " + currentPageId);
                    
                    if (currentPageId <= 0) {
                        System.err.println("Invalid page ID in branch: " + currentPageId);
                        return results; // Return empty results
                    }
                    
                    currentPage = pageManager.readPage(currentPageId);
                } catch (Exception e) {
                    System.err.println("Error following branch: " + e.getMessage());
                    e.printStackTrace();
                    return results; // Return empty results
                }
            }
            
            System.out.println("Found leaf page: " + currentPageId + " with " + currentPage.count() + " elements");
            
            // Debug - dump all keys in the leaf
            for (int i = 0; i < currentPage.count(); i++) {
                Element element = currentPage.element(i);
                if (element != null && element.key() != null) {
                    System.out.println("  Key at " + i + ": '" + new String(element.key()) + "'");
                }
            }
            
            // Now at leaf node, start collecting records
            boolean done = false;
            while (!done && currentPage != null) {
                int count = currentPage.count();
                
                for (int i = 0; i < count; i++) {
                    Element element = currentPage.element(i);
                    if (element == null) {
                        System.err.println("Null element at index " + i);
                        continue;
                    }
                    
                    byte[] key = element.key();
                    if (key == null) {
                        System.err.println("Null key at index " + i);
                        continue;
                    }
                    
                    System.out.println("Checking key: '" + new String(key) + "'");
                    
                    // Check if we've gone past the end key
                    if (endKey != null && compareKeys(key, endKey) > 0) {
                        System.out.println("  Exceeded end key, stopping scan");
                        done = true;
                        break;
                    }
                    
                    // Skip keys before start key
                    if (compareKeys(key, startKey) < 0) {
                        System.out.println("  Skipping key before start key");
                        continue;
                    }
                    
                    // Found a key in range, add to results
                    System.out.println("  Key in range, adding to results");
                    byte[] valueBytes;
                    try {
                        if (element.hasOverflow()) {
                            // Get value from overflow pages
                            byte[] overflowBytes = element.value();
                            if (overflowBytes == null || overflowBytes.length < 8) {
                                System.err.println("Invalid overflow reference");
                                continue;
                            }
                            
                            ByteBuffer buffer = ByteBuffer.wrap(overflowBytes);
                            buffer.rewind(); // Reset the buffer position
                            long overflowPageId = buffer.getLong();
                            
                            if (overflowPageId <= 0) {
                                System.err.println("Invalid overflow page ID: " + overflowPageId);
                                continue;
                            }
                            
                            valueBytes = readFromOverflowPages(overflowPageId);
                        } else {
                            // Regular inline value
                            valueBytes = element.value();
                        }
                        
                        if (valueBytes == null || valueBytes.length == 0) {
                            System.err.println("Empty value for key: " + new String(key));
                            continue;
                        }
                    } catch (IOException e) {
                        System.err.println("Error reading value during scan: " + e.getMessage());
                        e.printStackTrace();
                        continue; // Skip this record and continue with next
                    }
                    
                    Map<String, Object> value = valueSerializer.deserialize(valueBytes);
                    if (value == null) {
                        System.err.println("Failed to deserialize value for key: " + new String(key));
                        continue;
                    }
                    
                    // Apply column projection if needed
                    if (columns != null && !columns.isEmpty()) {
                        Map<String, Object> projectedValue = new HashMap<>();
                        for (String column : columns) {
                            if (value.containsKey(column)) {
                                projectedValue.put(column, value.get(column));
                            }
                        }
                        value = projectedValue;
                    }
                    
                    results.add(new Record(key, value));
                }
                
                // In a real implementation, we would follow the next leaf pointer here
                // Since we don't have leaf links in this implementation, we just stop
                done = true;
            }
            
            System.out.println("Scan completed. Found " + results.size() + " results");
            return results;
        } catch (Exception e) {
            System.err.println("Exception during scan: " + e.getMessage());
            e.printStackTrace();
            throw new IOException("Scan operation failed", e);
        }
    }
    
    @Override
    public void delete(byte[] key) throws IOException {
        // This is a simplified implementation that doesn't handle rebalancing
        // Start at the root
        long currentPageId = rootPageId;
        Page currentPage = pageManager.readPage(currentPageId);
        
        // Traverse down to the leaf node
        while (currentPage.isBranch()) {
            int index = findChildIndex(currentPage, key);
            Element element = currentPage.element(index);
            
            ByteBuffer buffer = ByteBuffer.wrap(element.value());
            currentPageId = buffer.getLong();
            currentPage = pageManager.readPage(currentPageId);
        }
        
        // Now at leaf node, search for the key and remove it
        // Note: This is a simplified approach. A real implementation would
        // handle rebalancing and page merges when a node becomes too empty.
        int count = currentPage.count();
        for (int i = 0; i < count; i++) {
            Element element = currentPage.element(i);
            if (Arrays.equals(key, element.key())) {
                // Found the key, remove it
                // For simplicity, we'll just recreate the page without this key
                // A real implementation would have a more efficient removal method
                
                // Extract all keys and values except the one to delete
                List<byte[]> keys = new ArrayList<>(count - 1);
                List<byte[]> values = new ArrayList<>(count - 1);
                
                for (int j = 0; j < count; j++) {
                    if (j != i) {
                        Element elem = currentPage.element(j);
                        keys.add(elem.key());
                        values.add(elem.value());
                    }
                }
                
                // Clear the page and reinsert all elements
                currentPage.setCount(0);
                for (int j = 0; j < keys.size(); j++) {
                    currentPage.putElement(keys.get(j), values.get(j));
                }
                
                // Write the updated page
                pageManager.writePage(currentPage);
                return;
            }
        }
    }
    
    @Override
    public void writeBatch(List<Record> records) throws IOException {
        // Simple implementation: just write each record individually
        // A more optimized implementation would sort and bulk-load
        for (Record record : records) {
            write(record.getKey(), record.getValue());
        }
    }
    
    @Override
    public void close() throws IOException {
        pageManager.sync();
        pageManager.close();
    }
    
    /**
     * Utility class for serializing/deserializing map values to/from byte arrays
     */
    private static class ValueSerializer {
        /**
         * Serializes a map to a byte array
         */
        public byte[] serialize(Map<String, Object> value) {
            try {
                if (value == null || value.isEmpty()) {
                    // Return minimal valid buffer if no entries
                    ByteBuffer emptyBuffer = ByteBuffer.allocate(4);
                    emptyBuffer.putInt(0); // Zero entries
                    return emptyBuffer.array();
                }
                
                // Simple serialization format:
                // [count][key1 length][key1][type1][value1]...[keyN length][keyN][typeN][valueN]
                
                // First pass: calculate size
                int size = 4; // Initial count (4 bytes)
                
                for (Map.Entry<String, Object> entry : value.entrySet()) {
                    if (entry.getKey() == null) {
                        // Skip null keys
                        System.err.println("Warning: Skipping null key in serialization");
                        continue;
                    }
                    
                    String key = entry.getKey();
                    Object val = entry.getValue();
                    
                    size += 4; // Key length (4 bytes)
                    size += key.getBytes(StandardCharsets.UTF_8).length; // Key bytes
                    size += 1; // Type (1 byte)
                    
                    // Value size depends on type
                    if (val == null) {
                        // No additional bytes for null
                    } else if (val instanceof String) {
                        size += 4; // String length (4 bytes)
                        size += ((String) val).getBytes(StandardCharsets.UTF_8).length; // String bytes
                    } else if (val instanceof Integer) {
                        size += 4; // Integer (4 bytes)
                    } else if (val instanceof Long) {
                        size += 8; // Long (8 bytes)
                    } else if (val instanceof Float) {
                        size += 4; // Float (4 bytes)
                    } else if (val instanceof Double) {
                        size += 8; // Double (8 bytes)
                    } else if (val instanceof Boolean) {
                        size += 1; // Boolean (1 byte)
                    } else {
                        // Unsupported type, convert to string
                        String strVal = val.toString();
                        size += 4; // String length (4 bytes)
                        size += strVal.getBytes(StandardCharsets.UTF_8).length; // String bytes
                    }
                }
                
                // Allocate buffer and write data
                ByteBuffer buffer = ByteBuffer.allocate(size);
                int validEntries = 0;
                
                // Count valid entries for the header
                for (Map.Entry<String, Object> entry : value.entrySet()) {
                    if (entry.getKey() != null) {
                        validEntries++;
                    }
                }
                
                buffer.putInt(validEntries); // Number of entries
                
                for (Map.Entry<String, Object> entry : value.entrySet()) {
                    if (entry.getKey() == null) {
                        continue; // Skip null keys
                    }
                    
                    String key = entry.getKey();
                    Object val = entry.getValue();
                    byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
                    
                    buffer.putInt(keyBytes.length);
                    buffer.put(keyBytes);
                    
                    if (val == null) {
                        buffer.put((byte) 0); // Type 0: null
                    } else if (val instanceof String) {
                        buffer.put((byte) 6); // Type 6: String (match deserialize type code)
                        byte[] strBytes = ((String) val).getBytes(StandardCharsets.UTF_8);
                        buffer.putInt(strBytes.length);
                        buffer.put(strBytes);
                    } else if (val instanceof Integer) {
                        buffer.put((byte) 1); // Type 1: Integer (match deserialize type code)
                        buffer.putInt((Integer) val);
                    } else if (val instanceof Long) {
                        buffer.put((byte) 2); // Type 2: Long (match deserialize type code)
                        buffer.putLong((Long) val);
                    } else if (val instanceof Float) {
                        buffer.put((byte) 3); // Type 3: Float (match deserialize type code)
                        buffer.putFloat((Float) val);
                    } else if (val instanceof Double) {
                        buffer.put((byte) 4); // Type 4: Double (match deserialize type code)
                        buffer.putDouble((Double) val);
                    } else if (val instanceof Boolean) {
                        buffer.put((byte) 5); // Type 5: Boolean (match deserialize type code)
                        buffer.put(((Boolean) val) ? (byte) 1 : (byte) 0);
                    } else {
                        // Unsupported type, convert to string
                        buffer.put((byte) 6); // Type 6: String (match deserialize type code)
                        byte[] strBytes = val.toString().getBytes(StandardCharsets.UTF_8);
                        buffer.putInt(strBytes.length);
                        buffer.put(strBytes);
                    }
                }
                
                return buffer.array();
            } catch (Exception e) {
                System.err.println("Exception in serialize: " + e.getMessage());
                e.printStackTrace();
                // Return minimal valid buffer on error
                ByteBuffer errorBuffer = ByteBuffer.allocate(4);
                errorBuffer.putInt(0); // Zero entries
                return errorBuffer.array();
            }
        }
        
        /**
         * Deserializes a byte array to a map
         */
        public Map<String, Object> deserialize(byte[] bytes) {
            if (bytes == null || bytes.length == 0) {
                return new HashMap<>();
            }
            
            try {
                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                Map<String, Object> result = new HashMap<>();
                
                // Safety check for minimum size
                if (buffer.capacity() < 4) {
                    System.err.println("Warning: Invalid serialized data, too small: " + buffer.capacity() + " bytes");
                    return result; // Return empty map
                }
                
                // Check if buffer position is valid
                if (buffer.position() < 0 || buffer.position() >= buffer.capacity()) {
                    System.err.println("Warning: Invalid buffer position: " + buffer.position() + ", capacity: " + buffer.capacity());
                    return result; // Return empty map
                }
                
                // Try to read entry count, but with additional error handling
                int count;
                try {
                    count = buffer.getInt(); // Number of entries
                } catch (BufferUnderflowException e) {
                    System.err.println("Warning: Buffer underflow reading entry count");
                    return result; // Return empty map
                }
                
                // Sanity check on entry count
                if (count < 0 || count > 1000) { // Arbitrary limit
                    System.err.println("Warning: Invalid entry count in serialized data: " + count);
                    return result; // Return empty map
                }
                
                for (int i = 0; i < count; i++) {
                    // Verify we have at least 4 bytes for key length
                    if (buffer.remaining() < 4) {
                        System.err.println("Warning: Buffer underflow while reading key length at entry " + i);
                        break; // Break to return partial results
                    }
                    
                    // Read key length with error handling
                    int keyLength;
                    try {
                        keyLength = buffer.getInt();
                    } catch (BufferUnderflowException e) {
                        System.err.println("Warning: Error reading key length at entry " + i);
                        break; // Break to return partial results
                    }
                    
                    // Sanity check on key length
                    if (keyLength < 0 || keyLength > buffer.remaining() || keyLength > 10000) { // More reasonable max length
                        System.err.println("Warning: Invalid key length: " + keyLength + " at entry " + i);
                        break; // Break to return partial results
                    }
                    
                    // Read key bytes with error handling
                    byte[] keyBytes = new byte[keyLength];
                    try {
                        buffer.get(keyBytes);
                    } catch (BufferUnderflowException e) {
                        System.err.println("Warning: Error reading key bytes at entry " + i);
                        break; // Break to return partial results
                    }
                    
                    // Create string from bytes with error handling
                    String key;
                    try {
                        key = new String(keyBytes, StandardCharsets.UTF_8);
                    } catch (Exception e) {
                        System.err.println("Warning: Error creating string from key bytes at entry " + i);
                        break; // Break to return partial results
                    }
                    
                    // Verify we have at least 1 byte for type
                    if (buffer.remaining() < 1) {
                        System.err.println("Warning: Buffer underflow while reading type at entry " + i + " for key " + key);
                        break; // Break to return partial results
                    }
                    
                    // Read type with error handling
                    byte type;
                    try {
                        type = buffer.get();
                    } catch (BufferUnderflowException e) {
                        System.err.println("Warning: Error reading type at entry " + i + " for key " + key);
                        break; // Break to return partial results
                    }
                    
                    // Validate type is within expected range
                    if (type < 0 || type > 6) {
                        System.err.println("Warning: Type code " + type + " out of expected range (0-6) for key " + key);
                    }
                    
                    // Read value based on type
                    switch (type) {
                        case 0: // null
                            result.put(key, null);
                            break;
                        case 1: // Integer (Type 1)
                            if (buffer.remaining() < 4) {
                                System.err.println("Warning: Buffer underflow while reading Integer");
                                break;
                            }
                            result.put(key, buffer.getInt());
                            break;
                        case 2: // Long (Type 2)
                            if (buffer.remaining() < 8) {
                                System.err.println("Warning: Buffer underflow while reading Long");
                                break;
                            }
                            result.put(key, buffer.getLong());
                            break;
                        case 3: // Float (Type 3)
                            if (buffer.remaining() < 4) {
                                System.err.println("Warning: Buffer underflow while reading Float");
                                break;
                            }
                            result.put(key, buffer.getFloat());
                            break;
                        case 4: // Double (Type 4)
                            if (buffer.remaining() < 8) {
                                System.err.println("Warning: Buffer underflow while reading Double");
                                break;
                            }
                            result.put(key, buffer.getDouble());
                            break;
                        case 5: // Boolean (Type 5)
                            if (buffer.remaining() < 1) {
                                System.err.println("Warning: Buffer underflow while reading Boolean");
                                break;
                            }
                            result.put(key, buffer.get() != 0);
                            break;
                        case 6: // String (Type 6)
                            if (buffer.remaining() < 4) {
                                System.err.println("Warning: Buffer underflow while reading String length for key " + key);
                                break;
                            }
                            
                            int strLength;
                            try {
                                strLength = buffer.getInt();
                            } catch (BufferUnderflowException e) {
                                System.err.println("Warning: Error reading String length for key " + key);
                                break;
                            }
                            
                            // More reasonable maximum string length limit
                            if (strLength < 0 || strLength > buffer.remaining() || strLength > 1000000) {
                                System.err.println("Warning: Invalid String length: " + strLength + " for key " + key);
                                break;
                            }
                            
                            byte[] strBytes = new byte[strLength];
                            try {
                                buffer.get(strBytes);
                                result.put(key, new String(strBytes, StandardCharsets.UTF_8));
                            } catch (Exception e) {
                                System.err.println("Warning: Error reading or converting String value for key " + key + ": " + e.getMessage());
                            }
                            break;
                        default:
                            System.err.println("Warning: Unknown type: " + type + " for key " + key + ", trying to interpret as string");
                            // Try to interpret as string for backward compatibility, with better error handling
                            if (buffer.remaining() >= 4) {
                                try {
                                    int unknownStrLength = buffer.getInt();
                                    // More reasonable string length limit
                                    if (unknownStrLength >= 0 && unknownStrLength <= buffer.remaining() && unknownStrLength <= 1000000) {
                                        byte[] unknownStrBytes = new byte[unknownStrLength];
                                        buffer.get(unknownStrBytes);
                                        result.put(key, new String(unknownStrBytes, StandardCharsets.UTF_8));
                                    } else {
                                        System.err.println("Warning: Invalid string length for unknown type: " + unknownStrLength + " for key " + key);
                                    }
                                } catch (Exception e) {
                                    System.err.println("Failed to recover unknown type for key " + key + ": " + e.getMessage());
                                }
                            } else {
                                System.err.println("Warning: Not enough remaining bytes to recover unknown type for key " + key);
                            }
                    }
                }
                
                return result;
            } catch (BufferUnderflowException e) {
                System.err.println("Buffer underflow exception during deserialization: " + e.getMessage());
                return new HashMap<>(); // Return empty map on error
            } catch (Exception e) {
                System.err.println("Exception during deserialization: " + e.getMessage());
                return new HashMap<>(); // Return empty map on error
            }
        }
    }
}
