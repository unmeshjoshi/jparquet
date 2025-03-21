package com.jparque.examples;

import com.jparque.table.Table;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Scanner;

/**
 * Example application that demonstrates how to use the Table class
 * for storing and retrieving customer profiles.
 */
public class CustomerProfileExample {
    
    private Table customerTable;
    private Path dataDir;
    
    /**
     * Initialize the example with a data directory.
     * 
     * @param dataDirectoryPath Path to the data directory
     * @throws IOException If directory creation fails
     */
    public CustomerProfileExample(String dataDirectoryPath) throws IOException {
        this.dataDir = Paths.get(dataDirectoryPath);
        
        // Create the data directory if it doesn't exist
        Files.createDirectories(dataDir);
        
        // Initialize the customer table
        this.customerTable = new Table(dataDir, "customers");
    }
    
    /**
     * Add a new customer profile.
     * 
     * @param customerId Customer ID (will be used as the key)
     * @param name Customer name
     * @param email Customer email
     * @param phone Customer phone
     * @param address Customer address
     * @param age Customer age
     * @throws IOException If insert fails
     */
    public void addCustomer(String customerId, String name, String email, 
                            String phone, String address, int age) throws IOException {
        Map<String, Object> customer = new HashMap<>();
        customer.put("name", name);
        customer.put("email", email);
        customer.put("phone", phone);
        customer.put("address", address);
        customer.put("age", age);
        customer.put("created_at", System.currentTimeMillis());
        
        customerTable.insert(customerId, customer);
        System.out.println("Customer " + customerId + " added successfully.");
    }
    
    /**
     * Find a customer by ID.
     * 
     * @param customerId Customer ID to look up
     * @throws IOException If lookup fails
     */
    public void findCustomer(String customerId) throws IOException {
        Optional<Map<String, Object>> result = customerTable.find(customerId);
        
        if (result.isPresent()) {
            Map<String, Object> customer = result.get();
            System.out.println("\nCustomer Profile: " + customerId);
            System.out.println("------------------------------");
            for (Map.Entry<String, Object> entry : customer.entrySet()) {
                System.out.println(entry.getKey() + ": " + entry.getValue());
            }
            System.out.println("------------------------------");
        } else {
            System.out.println("Customer not found: " + customerId);
        }
    }
    
    /**
     * Update a customer's profile.
     * 
     * @param customerId Customer ID to update
     * @param fieldsToUpdate Map of field names to new values
     * @throws IOException If update fails
     */
    public void updateCustomer(String customerId, Map<String, Object> fieldsToUpdate) throws IOException {
        Optional<Map<String, Object>> existingCustomer = customerTable.find(customerId);
        
        if (existingCustomer.isPresent()) {
            Map<String, Object> updatedCustomer = existingCustomer.get();
            
            // Update the fields
            updatedCustomer.putAll(fieldsToUpdate);
            updatedCustomer.put("updated_at", System.currentTimeMillis());
            
            // Save the updated customer
            customerTable.insert(customerId, updatedCustomer);
            System.out.println("Customer " + customerId + " updated successfully.");
        } else {
            System.out.println("Customer not found: " + customerId);
        }
    }
    
    /**
     * Delete a customer.
     * 
     * @param customerId Customer ID to delete
     * @throws IOException If deletion fails
     */
    public void deleteCustomer(String customerId) throws IOException {
        // First check if the customer exists
        Optional<Map<String, Object>> existingCustomer = customerTable.find(customerId);
        
        if (existingCustomer.isPresent()) {
            customerTable.delete(customerId);
            System.out.println("Customer " + customerId + " deleted successfully.");
        } else {
            System.out.println("Customer not found: " + customerId);
        }
    }
    
    /**
     * List all customers with IDs in a specific range.
     * 
     * @param startId Start of the ID range (inclusive)
     * @param endId End of the ID range (exclusive)
     * @throws IOException If scan fails
     */
    public void listCustomers(String startId, String endId) throws IOException {
        Map<String, Map<String, Object>> results = customerTable.scan(startId, endId, null);
        
        if (results.isEmpty()) {
            System.out.println("No customers found in the specified range.");
            return;
        }
        
        System.out.println("\nCustomer List (" + results.size() + " customers)");
        System.out.println("------------------------------");
        
        for (Map.Entry<String, Map<String, Object>> entry : results.entrySet()) {
            String customerId = entry.getKey();
            Map<String, Object> customer = entry.getValue();
            
            System.out.println("ID: " + customerId);
            System.out.println("Name: " + customer.get("name"));
            System.out.println("Email: " + customer.get("email"));
            System.out.println("------------------------------");
        }
    }
    
    /**
     * Close the customer table and release resources.
     * 
     * @throws IOException If close fails
     */
    public void close() throws IOException {
        if (customerTable != null) {
            customerTable.close();
            System.out.println("Customer database closed.");
        }
    }
    
    /**
     * Run a simple interactive demo of the customer profile management.
     */
    public void runDemo() throws IOException {
        Scanner scanner = new Scanner(System.in);
        boolean running = true;
        
        System.out.println("Customer Profile Management Demo");
        System.out.println("================================");
        
        // Add some sample customers
        addSampleCustomers();
        
        while (running) {
            System.out.println("\nChoose an operation:");
            System.out.println("1. Add a customer");
            System.out.println("2. Find a customer");
            System.out.println("3. Update a customer");
            System.out.println("4. Delete a customer");
            System.out.println("5. List customers");
            System.out.println("6. Exit");
            System.out.print("Enter your choice (1-6): ");
            
            String choice = scanner.nextLine();
            
            try {
                switch (choice) {
                    case "1":
                        addCustomerInteractive(scanner);
                        break;
                    case "2":
                        findCustomerInteractive(scanner);
                        break;
                    case "3":
                        updateCustomerInteractive(scanner);
                        break;
                    case "4":
                        deleteCustomerInteractive(scanner);
                        break;
                    case "5":
                        listCustomersInteractive(scanner);
                        break;
                    case "6":
                        running = false;
                        break;
                    default:
                        System.out.println("Invalid choice. Please try again.");
                }
            } catch (Exception e) {
                System.out.println("Error: " + e.getMessage());
            }
        }
        
        close();
    }
    
    private void addSampleCustomers() throws IOException {
        // Add some sample customers if the database is empty
        if (customerTable.scan("customer:0000", "customer:zzzz", null).isEmpty()) {
            addCustomer("customer:1001", "John Doe", "john.doe@example.com", 
                    "555-123-4567", "123 Main St, Anytown, USA", 35);
            
            addCustomer("customer:1002", "Jane Smith", "jane.smith@example.com",
                    "555-987-6543", "456 Oak Ave, Othertown, USA", 28);
            
            addCustomer("customer:1003", "Bob Johnson", "bob.johnson@example.com",
                    "555-555-5555", "789 Elm St, Somewhere, USA", 42);
        }
    }
    
    private void addCustomerInteractive(Scanner scanner) throws IOException {
        System.out.print("Enter customer ID (e.g., customer:1004): ");
        String customerId = scanner.nextLine();
        
        System.out.print("Enter name: ");
        String name = scanner.nextLine();
        
        System.out.print("Enter email: ");
        String email = scanner.nextLine();
        
        System.out.print("Enter phone: ");
        String phone = scanner.nextLine();
        
        System.out.print("Enter address: ");
        String address = scanner.nextLine();
        
        System.out.print("Enter age: ");
        int age = Integer.parseInt(scanner.nextLine());
        
        addCustomer(customerId, name, email, phone, address, age);
    }
    
    private void findCustomerInteractive(Scanner scanner) throws IOException {
        System.out.print("Enter customer ID to find: ");
        String customerId = scanner.nextLine();
        
        findCustomer(customerId);
    }
    
    private void updateCustomerInteractive(Scanner scanner) throws IOException {
        System.out.print("Enter customer ID to update: ");
        String customerId = scanner.nextLine();
        
        Optional<Map<String, Object>> existingCustomer = customerTable.find(customerId);
        if (!existingCustomer.isPresent()) {
            System.out.println("Customer not found: " + customerId);
            return;
        }
        
        Map<String, Object> fieldsToUpdate = new HashMap<>();
        
        System.out.println("Enter new values (leave blank to keep current value):");
        
        System.out.print("Name [" + existingCustomer.get().get("name") + "]: ");
        String name = scanner.nextLine();
        if (!name.isEmpty()) {
            fieldsToUpdate.put("name", name);
        }
        
        System.out.print("Email [" + existingCustomer.get().get("email") + "]: ");
        String email = scanner.nextLine();
        if (!email.isEmpty()) {
            fieldsToUpdate.put("email", email);
        }
        
        System.out.print("Phone [" + existingCustomer.get().get("phone") + "]: ");
        String phone = scanner.nextLine();
        if (!phone.isEmpty()) {
            fieldsToUpdate.put("phone", phone);
        }
        
        System.out.print("Address [" + existingCustomer.get().get("address") + "]: ");
        String address = scanner.nextLine();
        if (!address.isEmpty()) {
            fieldsToUpdate.put("address", address);
        }
        
        System.out.print("Age [" + existingCustomer.get().get("age") + "]: ");
        String ageStr = scanner.nextLine();
        if (!ageStr.isEmpty()) {
            fieldsToUpdate.put("age", Integer.parseInt(ageStr));
        }
        
        updateCustomer(customerId, fieldsToUpdate);
    }
    
    private void deleteCustomerInteractive(Scanner scanner) throws IOException {
        System.out.print("Enter customer ID to delete: ");
        String customerId = scanner.nextLine();
        
        System.out.print("Are you sure you want to delete this customer? (y/n): ");
        String confirm = scanner.nextLine();
        
        if (confirm.equalsIgnoreCase("y")) {
            deleteCustomer(customerId);
        } else {
            System.out.println("Deletion cancelled.");
        }
    }
    
    private void listCustomersInteractive(Scanner scanner) throws IOException {
        System.out.print("Enter start ID (press Enter for first): ");
        String startId = scanner.nextLine();
        if (startId.isEmpty()) {
            startId = "customer:0000";
        }
        
        System.out.print("Enter end ID (press Enter for last): ");
        String endId = scanner.nextLine();
        if (endId.isEmpty()) {
            endId = "customer:zzzz";
        }
        
        listCustomers(startId, endId);
    }
    
    /**
     * Main method to run the example.
     */
    public static void main(String[] args) {
        try {
            // Use a temporary directory for the demo
            String dataDir = System.getProperty("java.io.tmpdir") + "/customer-profile-demo";
            CustomerProfileExample example = new CustomerProfileExample(dataDir);
            
            example.runDemo();
        } catch (Exception e) {
            System.err.println("Error running example: " + e.getMessage());
            e.printStackTrace();
        }
    }
} 