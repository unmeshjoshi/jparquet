package com.jparque.btree.page;

import com.jparque.btree.page.Page;
import com.jparque.btree.page.PageManager;
import com.jparque.btree.page.Element;

import com.nimbusds.jose.shaded.json.JSONUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

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
    
    @Test
    public void shouldStoreAndRetrieveCustomerProfiles() throws IOException {
        // Given: a page for storing customer profiles
        long pageId = pageManager.allocatePage();
        Page customerPage = createPage(pageId);
        
        // When: customer profiles are stored in the page
        Map<String, Map<String, String>> customerProfiles = createCustomerProfiles();
        storeCustomerProfiles(customerPage, customerProfiles);
        pageManager.writePage(customerPage);
        
        // Then: the profiles can be retrieved and match the original data
        verifyCustomerProfilesAreRetrievedCorrectly(pageId, customerProfiles);
    }
    
    /**
     * Creates a new page with the given ID.
     */
    private Page createPage(long pageId) {
        Page page = new Page(PAGE_SIZE);
        page.setId(pageId);
        return page;
    }
    
    /**
     * Creates a set of sample customer profiles for testing.
     */
    private Map<String, Map<String, String>> createCustomerProfiles() {
        Map<String, Map<String, String>> customerProfiles = new HashMap<>();
        
        // Customer 1
        Map<String, String> customer1 = new HashMap<>();
        customer1.put("name", "John Doe");
        customer1.put("email", "john.doe@example.com");
        customer1.put("phone", "555-123-4567");
        customer1.put("address", "123 Main St, Anytown, USA");
        customerProfiles.put("customer:1001", customer1);
        
        // Customer 2
        Map<String, String> customer2 = new HashMap<>();
        customer2.put("name", "Jane Smith");
        customer2.put("email", "jane.smith@example.com");
        customer2.put("phone", "555-987-6543");
        customer2.put("address", "456 Oak Ave, Othertown, USA");
        customerProfiles.put("customer:1002", customer2);
        
        // Customer 3
        Map<String, String> customer3 = new HashMap<>();
        customer3.put("name", "Bob Johnson");
        customer3.put("email", "bob.johnson@example.com");
        customer3.put("phone", "555-555-5555");
        customer3.put("address", "789 Elm St, Somewhere, USA");
        customerProfiles.put("customer:1003", customer3);
        
        return customerProfiles;
    }
    
    /**
     * Stores customer profiles in the given page.
     */
    private void storeCustomerProfiles(Page page, Map<String, Map<String, String>> customerProfiles) {
        for (Map.Entry<String, Map<String, String>> entry : customerProfiles.entrySet()) {
            String customerId = entry.getKey();
            Map<String, String> profile = entry.getValue();
            
            byte[] serializedData = serializeCustomerProfile(profile);
            page.putElement(customerId.getBytes(), serializedData);
        }
    }
    
    /**
     * Serializes a customer profile to a byte array.
     */
    private byte[] serializeCustomerProfile(Map<String, String> profile) {
        StringBuilder serializedProfile = new StringBuilder();

        for (Map.Entry<String, String> profileEntry : profile.entrySet()) {
            if (serializedProfile.length() > 0) {
                serializedProfile.append("|");
            }
            serializedProfile.append(profileEntry.getKey())
                    .append("=")
                    .append(profileEntry.getValue());
        }
        
        return serializedProfile.toString().getBytes();
    }
    
    /**
     * Verifies that customer profiles can be retrieved correctly from the page.
     */
    private void verifyCustomerProfilesAreRetrievedCorrectly(long pageId, 
            Map<String, Map<String, String>> originalProfiles) throws IOException {
        // Read the page back
        Page readPage = pageManager.readPage(pageId);
        
        // Verify the page was read correctly
        assertEquals(pageId, readPage.id());
        assertEquals(originalProfiles.size(), readPage.count());
        
        // Verify each customer profile
        for (String customerId : originalProfiles.keySet()) {
            Element element = findCustomerElement(readPage, customerId);
            assertNotNull(element, "Customer profile for " + customerId + " not found");
            
            Map<String, String> retrievedProfile = deserializeCustomerProfile(element.value());
            Map<String, String> originalProfile = originalProfiles.get(customerId);
            
            verifyProfileMatches(customerId, originalProfile, retrievedProfile);
        }
    }
    
    /**
     * Finds the element containing the customer profile with the given ID.
     */
    private Element findCustomerElement(Page page, String customerId) {
        byte[] customerIdBytes = customerId.getBytes();
        
        for (int i = 0; i < page.count(); i++) {
            Element element = page.element(i);
            if (Arrays.equals(element.key(), customerIdBytes)) {
                return element;
            }
        }
        
        return null;
    }
    
    /**
     * Deserializes a customer profile from a byte array.
     */
    private Map<String, String> deserializeCustomerProfile(byte[] data) {
        String serializedProfile = new String(data);
        Map<String, String> profile = new HashMap<>();
        
        for (String pair : serializedProfile.split("\\|")) {
            String[] keyValue = pair.split("=", 2);
            if (keyValue.length == 2) {
                profile.put(keyValue[0], keyValue[1]);
            }
        }
        
        return profile;
    }
    
    /**
     * Verifies that a retrieved profile matches the original.
     */
    private void verifyProfileMatches(String customerId, Map<String, String> originalProfile, 
            Map<String, String> retrievedProfile) {
        assertEquals(originalProfile.size(), retrievedProfile.size(), 
                "Profile field count mismatch for " + customerId);
        
        for (Map.Entry<String, String> entry : originalProfile.entrySet()) {
            String fieldName = entry.getKey();
            String expectedValue = entry.getValue();
            String actualValue = retrievedProfile.get(fieldName);
            
            assertEquals(expectedValue, actualValue,
                    "Profile field value mismatch for " + customerId + "." + fieldName);
        }
    }
}
