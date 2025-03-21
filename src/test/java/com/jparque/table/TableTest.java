package com.jparque.table;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

public class TableTest {
    
    @TempDir
    Path tempDir;
    
    private Table customerTable;
    
    @BeforeEach
    public void setUp() throws IOException {
        // Create a new customer table
        customerTable = new Table(tempDir, "customers");
    }
    
    @AfterEach
    public void tearDown() throws IOException {
        // Close the table
        if (customerTable != null) {
            customerTable.close();
        }
    }
    
    @Test
    public void shouldStoreAndRetrieveCustomerData() throws IOException {
        // Create a customer profile
        Map<String, Object> customer1 = new HashMap<>();
        customer1.put("name", "John Doe");
        customer1.put("email", "john.doe@example.com");
        customer1.put("phone", "555-123-4567");
        customer1.put("address", "123 Main St, Anytown, USA");
        customer1.put("age", 35);
        customer1.put("member_since", "2022-01-15");
        
        // Insert the customer
        String customerId = "customer:1001";
        customerTable.insert(customerId, customer1);
        
        // Retrieve the customer
        Optional<Map<String, Object>> retrievedCustomer = customerTable.find(customerId);
        
        // Verify the customer was retrieved successfully
        assertTrue(retrievedCustomer.isPresent(), "Customer should be found");
        
        // Verify the customer data
        Map<String, Object> customerData = retrievedCustomer.get();
        assertEquals(customer1.size(), customerData.size(), "Field count should match");
        assertEquals("John Doe", customerData.get("name"));
        assertEquals("john.doe@example.com", customerData.get("email"));
        assertEquals("555-123-4567", customerData.get("phone"));
        assertEquals("123 Main St, Anytown, USA", customerData.get("address"));
        assertEquals(35, customerData.get("age"));
        assertEquals("2022-01-15", customerData.get("member_since"));
    }
    
    @Test
    public void shouldHandleMultipleCustomers() throws IOException {
        // Create multiple customer profiles
        Map<String, Map<String, Object>> customers = new HashMap<>();
        
        // Customer 1
        Map<String, Object> customer1 = new HashMap<>();
        customer1.put("name", "John Doe");
        customer1.put("email", "john.doe@example.com");
        customer1.put("phone", "555-123-4567");
        customer1.put("address", "123 Main St, Anytown, USA");
        customer1.put("age", 35);
        customers.put("customer:1001", customer1);
        
        // Customer 2
        Map<String, Object> customer2 = new HashMap<>();
        customer2.put("name", "Jane Smith");
        customer2.put("email", "jane.smith@example.com");
        customer2.put("phone", "555-987-6543");
        customer2.put("address", "456 Oak Ave, Othertown, USA");
        customer2.put("age", 28);
        customers.put("customer:1002", customer2);
        
        // Customer 3
        Map<String, Object> customer3 = new HashMap<>();
        customer3.put("name", "Bob Johnson");
        customer3.put("email", "bob.johnson@example.com");
        customer3.put("phone", "555-555-5555");
        customer3.put("address", "789 Elm St, Somewhere, USA");
        customer3.put("age", 42);
        customers.put("customer:1003", customer3);
        
        // Insert all customers in a batch
        customerTable.batchInsert(customers);
        
        // Verify each customer can be retrieved
        for (String customerId : customers.keySet()) {
            Optional<Map<String, Object>> retrievedCustomer = customerTable.find(customerId);
            assertTrue(retrievedCustomer.isPresent(), "Customer " + customerId + " should be found");
            
            Map<String, Object> originalCustomer = customers.get(customerId);
            Map<String, Object> retrievedData = retrievedCustomer.get();
            
            // Verify all fields match
            assertEquals(originalCustomer.size(), retrievedData.size(), 
                    "Field count should match for " + customerId);
            
            for (Map.Entry<String, Object> entry : originalCustomer.entrySet()) {
                assertEquals(entry.getValue(), retrievedData.get(entry.getKey()),
                        "Field value mismatch for " + customerId + "." + entry.getKey());
            }
        }
    }
    
    @Test
    public void shouldScanCustomerRange() throws IOException {
        // Create multiple customer profiles
        Map<String, Map<String, Object>> customers = new HashMap<>();
        
        // Add 10 customers with sequential IDs
        for (int i = 1; i <= 10; i++) {
            String customerId = String.format("customer:%04d", i);
            Map<String, Object> customer = new HashMap<>();
            customer.put("name", "Customer " + i);
            customer.put("email", "customer" + i + "@example.com");
            customer.put("age", 25 + i);
            customers.put(customerId, customer);
        }
        
        // Insert all customers
        customerTable.batchInsert(customers);
        
        // Scan for customers in range 3-7 (inclusive to exclusive)
        Map<String, Map<String, Object>> rangeResult = customerTable.scan(
                "customer:0003", "customer:0008", null);
        
        // Verify correct number of results
        assertEquals(5, rangeResult.size(), "Should return 5 customers");
        
        // Verify expected customers are in the result
        for (int i = 3; i <= 7; i++) {
            String customerId = String.format("customer:%04d", i);
            assertTrue(rangeResult.containsKey(customerId), 
                    "Result should contain " + customerId);
            
            Map<String, Object> customerData = rangeResult.get(customerId);
            assertEquals("Customer " + i, customerData.get("name"), 
                    "Customer name should match");
            assertEquals(25 + i, customerData.get("age"), 
                    "Customer age should match");
        }
    }
    
    @Test
    public void shouldSelectSpecificColumns() throws IOException {
        // Create a customer with multiple fields
        Map<String, Object> customer = new HashMap<>();
        customer.put("name", "John Doe");
        customer.put("email", "john.doe@example.com");
        customer.put("phone", "555-123-4567");
        customer.put("address", "123 Main St, Anytown, USA");
        customer.put("age", 35);
        customer.put("member_since", "2022-01-15");
        customer.put("preferences", "{'theme':'dark','notifications':true}");
        
        // Insert the customer
        String customerId = "customer:1001";
        customerTable.insert(customerId, customer);
        
        // Retrieve only specific columns
        List<String> columns = new ArrayList<>();
        columns.add("name");
        columns.add("email");
        columns.add("age");
        
        Map<String, Map<String, Object>> result = customerTable.scan(customerId, "customer:1002", columns);
        
        // Verify the retrieved data
        assertTrue(result.containsKey(customerId), "Customer should be in result");
        Map<String, Object> projectedData = result.get(customerId);
        
        // Verify only requested columns are present
        assertEquals(3, projectedData.size(), "Should only have 3 requested fields");
        assertEquals("John Doe", projectedData.get("name"));
        assertEquals("john.doe@example.com", projectedData.get("email"));
        assertEquals(35, projectedData.get("age"));
        
        // Verify other fields are not present
        assertFalse(projectedData.containsKey("phone"));
        assertFalse(projectedData.containsKey("address"));
        assertFalse(projectedData.containsKey("member_since"));
        assertFalse(projectedData.containsKey("preferences"));
    }
    
    @Test
    public void shouldUpdateAndDeleteCustomers() throws IOException {
        // Create a customer profile
        Map<String, Object> customer = new HashMap<>();
        customer.put("name", "John Doe");
        customer.put("email", "john.doe@example.com");
        customer.put("phone", "555-123-4567");
        customer.put("age", 35);
        
        // Insert the customer
        String customerId = "customer:1001";
        customerTable.insert(customerId, customer);
        
        // Update the customer
        Map<String, Object> updatedCustomer = new HashMap<>();
        updatedCustomer.put("name", "John Doe Jr.");
        updatedCustomer.put("email", "john.jr@example.com");
        updatedCustomer.put("phone", "555-123-4567");
        updatedCustomer.put("age", 36);
        updatedCustomer.put("address", "123 Main St, Anytown, USA");
        
        // Insert with same key to update
        customerTable.insert(customerId, updatedCustomer);
        
        // Verify the update
        Optional<Map<String, Object>> retrieved = customerTable.find(customerId);
        assertTrue(retrieved.isPresent(), "Updated customer should be found");
        
        Map<String, Object> retrievedData = retrieved.get();
        assertEquals("John Doe Jr.", retrievedData.get("name"), "Name should be updated");
        assertEquals("john.jr@example.com", retrievedData.get("email"), "Email should be updated");
        assertEquals(36, retrievedData.get("age"), "Age should be updated");
        assertEquals("123 Main St, Anytown, USA", retrievedData.get("address"), "New field should be added");
        
        // Delete the customer
        customerTable.delete(customerId);
        
        // Verify the deletion
        Optional<Map<String, Object>> afterDelete = customerTable.find(customerId);
        assertFalse(afterDelete.isPresent(), "Customer should be deleted");
    }
} 