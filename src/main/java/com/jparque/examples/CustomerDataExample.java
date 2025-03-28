package com.jparque.examples;

import com.jparque.btree.BPlusTree;
import com.jparque.columnar.ColumnStore;
import com.jparque.common.schema.MessageType;
import com.jparque.common.schema.OriginalType;
import com.jparque.common.schema.Repetition;
import com.jparque.common.schema.Type;
import com.jparque.storage.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * This example demonstrates the differences between row-based storage (BPlusTree)
 * and column-based storage (ColumnStore) for customer data with different use cases.
 * 
 * Two scenarios are presented:
 * 1. OLTP Scenario: Efficient record lookup and updates for a customer service application
 * 2. OLAP Scenario: Analytical queries that process large amounts of data across columns
 */
public class CustomerDataExample {
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final int NUM_CUSTOMERS = 1000; // 10K customers for our example
    private static final int BATCH_SIZE = 1000;
    
    private final Path dataDir;
    private final MessageType customerSchema;
    
    // Storage engines
    private BPlusTree rowStore;
    private ColumnStore columnStore;
    
    public CustomerDataExample(Path baseDir) throws IOException {
        // Create data directory
        this.dataDir = baseDir.resolve("customer_data");
        Files.createDirectories(dataDir);
        
        // Define customer schema
        this.customerSchema = new MessageType.Builder("Customer")
            .addField("customerId", Type.INT64, Repetition.REQUIRED)
            .addField("name", Type.BINARY, Repetition.REQUIRED, OriginalType.UTF8)
            .addField("email", Type.BINARY, Repetition.OPTIONAL, OriginalType.UTF8)
            .addField("registrationDate", Type.BINARY, Repetition.REQUIRED, OriginalType.UTF8)
            .addField("lastLoginDate", Type.BINARY, Repetition.OPTIONAL, OriginalType.UTF8)
            .addField("totalSpend", Type.INT32, Repetition.REQUIRED)
            .addField("loyaltyPoints", Type.INT32, Repetition.REQUIRED)
            .addField("isActive", Type.INT32, Repetition.REQUIRED)
            .addField("countryCode", Type.BINARY, Repetition.REQUIRED, OriginalType.UTF8)
            .addField("tags", Type.BINARY, Repetition.OPTIONAL, OriginalType.UTF8)
            .build();
        
        // Initialize storage engines
        this.rowStore = new BPlusTree(dataDir.resolve("customer_rowstore.db"));
        this.columnStore = new ColumnStore(dataDir, "customer_columnstore", customerSchema);
    }
    
    /**
     * Generates random customer data.
     */
    public List<Record> generateCustomerData(int startId, int count) {
        List<Record> records = new ArrayList<>(count);
        Random random = new Random();
        String[] countries = {"US", "UK", "CA", "AU", "DE", "FR", "JP", "IN", "BR", "CN"};
        String[] tagOptions = {"premium", "new", "returning", "highValue", "inactive", "promotional", "churn-risk", "verified"};
        
        LocalDate now = LocalDate.now();
        
        for (int i = 0; i < count; i++) {
            int id = startId + i;
            String name = "Customer " + id;
            String email = "customer" + id + "@example.com";
            
            // Generate registration date (between 1-5 years ago)
            LocalDate registrationDate = now.minusDays(random.nextInt(365 * 5) + 1);
            
            // Some customers might not have logged in recently
            LocalDate lastLoginDate = random.nextInt(10) < 8 
                ? now.minusDays(random.nextInt(90)) 
                : null;
            
            int totalSpend = (int)(random.nextDouble() * 10000); // Up to $10,000
            int loyaltyPoints = random.nextInt(10000);
            int isActive = random.nextInt(10) < 8 ? 1 : 0; // 1 = active, 0 = inactive
            String countryCode = countries[random.nextInt(countries.length)];
            
            // Generate 0-3 random tags as a comma-separated string
            StringBuilder tagsBuilder = new StringBuilder();
            int numTags = random.nextInt(4);
            for (int t = 0; t < numTags; t++) {
                if (t > 0) tagsBuilder.append(',');
                tagsBuilder.append(tagOptions[random.nextInt(tagOptions.length)]);
            }
            String tags = tagsBuilder.toString();
            
            // Create customer record
            Map<String, Object> value = new HashMap<>();
            value.put("customerId", (long) id);
            value.put("name", name);
            value.put("email", email);
            value.put("registrationDate", registrationDate.format(DATE_FORMATTER));
            if (lastLoginDate != null) {
                value.put("lastLoginDate", lastLoginDate.format(DATE_FORMATTER));
            }
            value.put("totalSpend", totalSpend);
            value.put("loyaltyPoints", loyaltyPoints);
            value.put("isActive", isActive);
            value.put("countryCode", countryCode);
            
            value.put("tags", tags);
            
            // Create record with key as customerId
            byte[] key = ByteBuffer.allocate(Long.BYTES).putLong((long) id).array();
            records.add(new Record(key, value));
        }
        
        return records;
    }
    
    /**
     * Populates both row and column stores with the same data.
     */
    public void populateStores() throws IOException {
        System.out.println("Generating and storing " + NUM_CUSTOMERS + " customer records...");
        
        for (int i = 0; i < NUM_CUSTOMERS; i += BATCH_SIZE) {
            int batchCount = Math.min(BATCH_SIZE, NUM_CUSTOMERS - i);
            List<Record> batch = generateCustomerData(i, batchCount);
            
            // Write to both stores
            System.out.printf("Writing batch %d/%d (customers %d-%d)...%n", 
                    (i / BATCH_SIZE) + 1, 
                    (NUM_CUSTOMERS / BATCH_SIZE), 
                    i, 
                    i + batchCount - 1);
            
            rowStore.writeBatch(batch);
            columnStore.writeBatch(batch);
        }
        
        System.out.println("Data population complete!");
    }
    
    /**
     * Scenario 1: OLTP - Customer Service Application
     * A customer service rep needs to look up a specific customer record
     * and see all their information at once.
     * 
     * Row-based storage excels at this type of access pattern.
     */
    public void demonstrateCustomerLookupScenario() throws IOException {
        System.out.println("\n----- SCENARIO 1: OLTP - Customer Lookup -----");
        System.out.println("Customer service rep needs to view complete customer profiles");
        
        // Pick a few random customer IDs
        Random random = new Random();
        List<Integer> customerIds = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            customerIds.add(random.nextInt(NUM_CUSTOMERS));
        }
        
        System.out.println("\nRow Store (BPlusTree) - Optimized for retrieving complete records:");
        long rowStoreStart = System.currentTimeMillis();
        
        for (int customerId : customerIds) {
            byte[] key = ByteBuffer.allocate(Long.BYTES).putLong((long) customerId).array();
            
            Optional<Map<String, Object>> result = rowStore.read(key);
            if (result.isPresent()) {
                Map<String, Object> customer = result.get();
                String name = (String) customer.get("name");
                System.out.printf("Found customer %d: %s%n", customerId, name);
            } else {
                System.out.printf("Customer %d not found%n", customerId);
            }
        }
        
        long rowStoreTime = System.currentTimeMillis() - rowStoreStart;
        System.out.printf("Total time for row store lookups: %d ms%n", rowStoreTime);
        
        System.out.println("\nColumn Store - Same operation (less optimal for this use case):");
        long colStoreStart = System.currentTimeMillis();
        
        for (int customerId : customerIds) {
            byte[] key = ByteBuffer.allocate(Long.BYTES).putLong((long) customerId).array();
            
            Optional<Map<String, Object>> result = columnStore.read(key);
            if (result.isPresent()) {
                Map<String, Object> customer = result.get();
                String name = (String) customer.get("name");
                System.out.printf("Found customer %d: %s%n", customerId, name);
            } else {
                System.out.printf("Customer %d not found%n", customerId);
            }
        }
        
        long colStoreTime = System.currentTimeMillis() - colStoreStart;
        System.out.printf("Total time for column store lookups: %d ms%n", colStoreTime);
        
        System.out.println("\nFor individual record lookups, row-based storage typically performs better");
        System.out.println("because all data for a single customer is stored together on disk.");
    }
    
    /**
     * Scenario 2: OLAP - Marketing Analytics
     * Find high-value customers who haven't logged in recently for a targeted campaign.
     * 
     * Column-based storage excels at this type of analytics query that only
     * needs specific columns across many records.
     */
    public void demonstrateAnalyticsScenario() throws IOException {
        System.out.println("\n----- SCENARIO 2: OLAP - Marketing Analytics -----");
        System.out.println("Finding high-value customers (>$500) who haven't logged in for 30+ days");
        
        byte[] startKey = ByteBuffer.allocate(Long.BYTES).putLong(0L).array();
        byte[] endKey = ByteBuffer.allocate(Long.BYTES).putLong((long) NUM_CUSTOMERS).array();
        
        // For analytics, we only need these specific columns
        List<String> columns = Arrays.asList("customerId", "totalSpend", "lastLoginDate", "email");
        
        System.out.println("\nRow Store - Scanning all records (less optimal for this use case):");
        long rowStoreStart = System.currentTimeMillis();
        List<Record> rowCandidates = new ArrayList<>();
        
        // With row store, we need to scan all records and filter in-memory
        List<Record> rowResults = rowStore.scan(startKey, endKey, null); // Need all columns since filtering happens after retrieval
        
        LocalDate cutoffDate = LocalDate.now().minusDays(30);
        for (Record record : rowResults) {
            Map<String, Object> data = record.getValue();
            // Add null check
            if (data == null || data.get("totalSpend") == null) {
                System.out.println("Warning: Invalid or corrupted row record: " + record.getKey());
                continue;
            }
            
            int totalSpend = (int) data.get("totalSpend");
            
            // Check total spend criteria
            if (totalSpend > 500) {
                String lastLoginDateStr = (String) data.get("lastLoginDate");
                
                // If last login date is missing or before cutoff date
                if (lastLoginDateStr == null) {
                    rowCandidates.add(record);
                } else {
                    LocalDate lastLogin = LocalDate.parse(lastLoginDateStr, DATE_FORMATTER);
                    if (lastLogin.isBefore(cutoffDate)) {
                        rowCandidates.add(record);
                    }
                }
            }
        }
        
        long rowStoreTime = System.currentTimeMillis() - rowStoreStart;
        System.out.printf("Found %d high-value inactive customers in %d ms%n", 
                rowCandidates.size(), rowStoreTime);
        
        
        System.out.println("\nColumn Store - Performing the same analysis (optimized for this use case):");
        long colStoreStart = System.currentTimeMillis();
        List<Record> colCandidates = new ArrayList<>();
        
        // With column store, we only need to retrieve the specific columns we're interested in
        List<Record> colResults = columnStore.scan(startKey, endKey, columns);
        
        for (Record record : colResults) {
            Map<String, Object> data = record.getValue();
            // Add null check
            if (data == null || data.get("totalSpend") == null) {
                System.out.println("Warning: Invalid or corrupted column record: " + record.getKey());
                continue;
            }
            
            int totalSpend = (int) data.get("totalSpend");
            
            // Check total spend criteria
            if (totalSpend > 500) {
                String lastLoginDateStr = (String) data.get("lastLoginDate");
                
                // If last login date is missing or before cutoff date
                if (lastLoginDateStr == null) {
                    colCandidates.add(record);
                } else {
                    LocalDate lastLogin = LocalDate.parse(lastLoginDateStr, DATE_FORMATTER);
                    if (lastLogin.isBefore(cutoffDate)) {
                        colCandidates.add(record);
                    }
                }
            }
        }
        
        long colStoreTime = System.currentTimeMillis() - colStoreStart;
        System.out.printf("Found %d high-value inactive customers in %d ms%n", 
                colCandidates.size(), colStoreTime);
        
        System.out.println("\nFor analytical queries that only need specific columns, column-based storage");
        System.out.println("typically performs much better because:");
        System.out.println(" - Only the needed columns are read from disk (less I/O)");
        System.out.println(" - Better compression ratios for similar column values");
        System.out.println(" - Column-based predicate pushdown can skip irrelevant data");
    }
    
    /**
     * Closes both storage engines.
     */
    public void close() throws IOException {
        if (rowStore != null) {
            rowStore.close();
        }
        if (columnStore != null) {
            columnStore.close();
        }
    }
    
    public static void main(String[] args) {
        try {
            // Create example with data directory in user home
            Path baseDir = Path.of(System.getProperty("user.home"), "jparque_example");
            CustomerDataExample example = new CustomerDataExample(baseDir);
            
            // Populate data stores
            example.populateStores();
            
            // Run our demonstration scenarios
            example.demonstrateCustomerLookupScenario();
            example.demonstrateAnalyticsScenario();
            
            // Clean up resources
            example.close();
            
            System.out.println("\nExample complete. Data stored in: " + baseDir);
            
        } catch (Exception e) {
            System.err.println("Error in customer data example: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
