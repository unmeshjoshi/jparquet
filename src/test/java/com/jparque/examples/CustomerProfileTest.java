package com.jparque.examples;

import com.jparque.table.Table;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class that demonstrates storing and retrieving customer profile data
 * using the Table class.
 */
public class CustomerProfileTest {
    
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
    public void shouldStoreAndRetrieveCustomerProfiles() throws IOException {
        // Create customer profiles with various data types
        Map<String, Map<String, Object>> customers = createSampleCustomers();
        
        // Insert all customers
        for (Map.Entry<String, Map<String, Object>> entry : customers.entrySet()) {
            customerTable.insert(entry.getKey(), entry.getValue());
            System.out.println("Inserted customer: " + entry.getKey());
        }
        
        // Retrieve and verify each customer
        for (String customerId : customers.keySet()) {
            Optional<Map<String, Object>> retrievedCustomer = customerTable.find(customerId);
            
            // Verify customer exists
            assertTrue(retrievedCustomer.isPresent(), "Customer " + customerId + " should be found");
            
            Map<String, Object> expectedData = customers.get(customerId);
            Map<String, Object> actualData = retrievedCustomer.get();
            
            // Print the retrieved data
            System.out.println("\nCustomer: " + customerId);
            for (Map.Entry<String, Object> field : actualData.entrySet()) {
                System.out.println("  " + field.getKey() + ": " + field.getValue());
            }
            
            // Verify all fields match
            assertEquals(expectedData.size(), actualData.size(), 
                    "Field count should match for " + customerId);
            
            for (Map.Entry<String, Object> field : expectedData.entrySet()) {
                String fieldName = field.getKey();
                Object expectedValue = field.getValue();
                Object actualValue = actualData.get(fieldName);
                
                assertEquals(expectedValue, actualValue, 
                        "Field " + fieldName + " value mismatch for " + customerId);
            }
        }
    }
    
    @Test
    public void shouldUpdateCustomerProfile() throws IOException {
        // Create a customer
        String customerId = "customer:1001";
        Map<String, Object> customer = new HashMap<>();
        customer.put("name", "John Doe");
        customer.put("email", "john.doe@example.com");
        customer.put("phone", "555-123-4567");
        customer.put("address", "123 Main St, Anytown, USA");
        customer.put("age", 35);
        customer.put("member_since", "2022-01-15");
        
        // Insert the customer
        customerTable.insert(customerId, customer);
        
        // Verify initial state
        Optional<Map<String, Object>> initial = customerTable.find(customerId);
        assertTrue(initial.isPresent(), "Customer should exist");
        assertEquals("John Doe", initial.get().get("name"), "Initial name should match");
        assertEquals(35, initial.get().get("age"), "Initial age should match");
        
        // Update the customer
        Map<String, Object> updatedData = new HashMap<>();
        updatedData.put("name", "John Doe Jr.");
        updatedData.put("age", 36);
        updatedData.put("address", "456 New St, Anytown, USA");
        updatedData.put("preferences", "{\"notifications\":true,\"theme\":\"dark\"}");
        
        // Create updated customer with all fields
        Map<String, Object> updatedCustomer = new HashMap<>(initial.get());
        updatedCustomer.putAll(updatedData);
        
        // Insert the updated customer
        customerTable.insert(customerId, updatedCustomer);
        
        // Verify the update
        Optional<Map<String, Object>> after = customerTable.find(customerId);
        assertTrue(after.isPresent(), "Customer should still exist after update");
        
        // Check the updated fields
        Map<String, Object> result = after.get();
        assertEquals("John Doe Jr.", result.get("name"), "Name should be updated");
        assertEquals(36, result.get("age"), "Age should be updated");
        assertEquals("456 New St, Anytown, USA", result.get("address"), "Address should be updated");
        assertEquals("{\"notifications\":true,\"theme\":\"dark\"}", result.get("preferences"), 
                "New field should be added");
        
        // Verify unchanged fields
        assertEquals("john.doe@example.com", result.get("email"), "Email should be unchanged");
        assertEquals("555-123-4567", result.get("phone"), "Phone should be unchanged");
        assertEquals("2022-01-15", result.get("member_since"), "Member since should be unchanged");
    }
    
    /**
     * Creates a set of sample customers with various data types.
     */
    private Map<String, Map<String, Object>> createSampleCustomers() {
        Map<String, Map<String, Object>> customers = new HashMap<>();
        
        // Customer 1 - Basic info
        Map<String, Object> customer1 = new HashMap<>();
        customer1.put("name", "John Doe");
        customer1.put("email", "john.doe@example.com");
        customer1.put("phone", "555-123-4567");
        customer1.put("address", "123 Main St, Anytown, USA");
        customer1.put("age", 35);
        customer1.put("member_since", "2022-01-15");
        customers.put("customer:1001", customer1);
        
        // Customer 2 - Premium customer with more fields
        Map<String, Object> customer2 = new HashMap<>();
        customer2.put("name", "Jane Smith");
        customer2.put("email", "jane.smith@example.com");
        customer2.put("phone", "555-987-6543");
        customer2.put("address", "456 Oak Ave, Othertown, USA");
        customer2.put("age", 28);
        customer2.put("member_since", "2021-03-10");
        customer2.put("premium_member", true);
        customer2.put("loyalty_points", 2500);
        customer2.put("last_purchase", "2023-11-28");
        customers.put("customer:1002", customer2);
        
        // Customer 3 - Business customer with different structure
        Map<String, Object> customer3 = new HashMap<>();
        customer3.put("name", "Acme Corporation");
        customer3.put("email", "contact@acmecorp.com");
        customer3.put("phone", "555-555-5555");
        customer3.put("address", "789 Business Blvd, Commerce City, USA");
        customer3.put("business_type", "Manufacturing");
        customer3.put("employee_count", 500);
        customer3.put("annual_revenue", 10000000);
        customer3.put("tax_id", "123-45-6789");
        customer3.put("account_manager", "Bob Johnson");
        customers.put("customer:1003", customer3);
        
        // Customer 4 - International customer
        Map<String, Object> customer4 = new HashMap<>();
        customer4.put("name", "Pierre Dupont");
        customer4.put("email", "pierre.dupont@example.fr");
        customer4.put("phone", "+33-1-23-45-67-89");
        customer4.put("address", "15 Rue de la Paix, Paris, France");
        customer4.put("age", 42);
        customer4.put("language", "French");
        customer4.put("shipping_zone", "Europe");
        customer4.put("currency", "EUR");
        customer4.put("tax_rate", 20.0);
        customers.put("customer:1004", customer4);
        
        // Customer 5 - Customer with preferences stored as JSON string
        Map<String, Object> customer5 = new HashMap<>();
        customer5.put("name", "Alex Kim");
        customer5.put("email", "alex.kim@example.com");
        customer5.put("phone", "555-222-3333");
        customer5.put("address", "101 Tech Lane, Silicon Valley, USA");
        customer5.put("age", 31);
        customer5.put("preferences", "{\"theme\":\"dark\",\"notifications\":true,\"newsletter\":false}");
        customer5.put("devices", "[\"mobile\",\"laptop\",\"tablet\"]");
        customer5.put("tags", "[\"tech\",\"early-adopter\",\"premium\"]");
        customers.put("customer:1005", customer5);
        
        return customers;
    }
} 