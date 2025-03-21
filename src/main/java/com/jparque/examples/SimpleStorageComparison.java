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
import java.util.*;

/**
 * A simple example that demonstrates the strengths of row-based storage (BPlusTree)
 * versus column-based storage (ColumnStore) using a small customer dataset.
 */
public class SimpleStorageComparison {

    public static void main(String[] args) throws IOException {
        // Create a temporary directory for our example
        Path tempDir = Files.createTempDirectory("storage_example");
        
        try {
            // Define a schema for our customer data
            MessageType schema = new MessageType.Builder("Customer")
                .addField("id", Type.INT32, Repetition.REQUIRED)
                .addField("name", Type.BINARY, Repetition.REQUIRED, OriginalType.UTF8)
                .addField("email", Type.BINARY, Repetition.REQUIRED, OriginalType.UTF8)
                .addField("age", Type.INT32, Repetition.REQUIRED)
                .addField("country", Type.BINARY, Repetition.REQUIRED, OriginalType.UTF8)
                .addField("balance", Type.INT32, Repetition.REQUIRED)
                .build();
            
            // Initialize both storage engines
            BPlusTree rowStore = new BPlusTree(tempDir.resolve("row_store.db"));
            ColumnStore columnStore = new ColumnStore(tempDir, "column_store.parquet", schema);
            
            // Generate and store some sample customer data
            List<Record> customers = generateSampleCustomerData(100);
            
            System.out.println("Storing 100 customer records in both storage engines...");
            
            // Write data to both stores
            for (Record record : customers) {
                rowStore.write(record.getKey(), record.getValue());
                columnStore.write(record.getKey(), record.getValue());
            }
            
            System.out.println("Data stored successfully!\n");
            
            // Scenario 1: Look up a specific customer (OLTP - Row Storage Advantage)
            System.out.println("===== Scenario 1: Customer Lookup (OLTP) =====");
            System.out.println("Finding a specific customer by ID (Customer #42)");
            
            byte[] lookupKey = ByteBuffer.allocate(4).putInt(42).array();
            
            // Measure row store performance
            long rowStart = System.nanoTime();
            Optional<Map<String, Object>> rowResult = rowStore.read(lookupKey);
            long rowTime = System.nanoTime() - rowStart;
            
            if (rowResult.isPresent()) {
                Map<String, Object> customer = rowResult.get();
                String name = (String) customer.get("name");
                String email = (String) customer.get("email");
                String country = (String) customer.get("country");
                System.out.println("Row Store found: " + name + " (" + email + ") from " + country);
                System.out.printf("Row Store lookup time: %.3f ms\n", rowTime / 1_000_000.0);
            }
            
            // Measure column store performance
            long colStart = System.nanoTime();
            Optional<Map<String, Object>> colResult = columnStore.read(lookupKey);
            long colTime = System.nanoTime() - colStart;
            
            if (colResult.isPresent()) {
                Map<String, Object> customer = colResult.get();
                String name = (String) customer.get("name");
                String email = (String) customer.get("email");
                String country = (String) customer.get("country");
                System.out.println("Column Store found: " + name + " (" + email + ") from " + country);
                System.out.printf("Column Store lookup time: %.3f ms\n", colTime / 1_000_000.0);
            }
            
            System.out.println("\nRow Store Advantage: For point queries retrieving complete records, " +
                    "row-based storage is typically faster because all fields are stored together.");
            
            // Scenario 2: Calculate average balance by country (OLAP - Column Storage Advantage)
            System.out.println("\n===== Scenario 2: Analytics Query (OLAP) =====");
            System.out.println("Calculating average customer balance by country");
            
            // Setup for scan
            byte[] startKey = ByteBuffer.allocate(4).putInt(0).array();
            byte[] endKey = ByteBuffer.allocate(4).putInt(Integer.MAX_VALUE).array();
            
            System.out.println("\nRow Store - Need to scan all columns:");
            long rowAnalyticsStart = System.nanoTime();
            
            // With row store we need all columns because data is stored by row
            Map<String, List<Integer>> rowCountryBalances = new HashMap<>();
            
            List<Record> rowRecords = rowStore.scan(startKey, endKey, null);
            for (Record record : rowRecords) {
                Map<String, Object> data = record.getValue();
                String country = (String) data.get("country");
                Integer balance = (Integer) data.get("balance");
                
                rowCountryBalances.computeIfAbsent(country, k -> new ArrayList<>()).add(balance);
            }
            
            // Calculate and display averages
            for (Map.Entry<String, List<Integer>> entry : rowCountryBalances.entrySet()) {
                String country = entry.getKey();
                List<Integer> balances = entry.getValue();
                double average = balances.stream().mapToDouble(Integer::intValue).average().orElse(0.0);
                System.out.printf("Country: %s, Average Balance: $%.2f (from %d customers)\n", 
                        country, average, balances.size());
            }
            
            long rowAnalyticsTime = System.nanoTime() - rowAnalyticsStart;
            System.out.printf("Row Store analytics time: %.3f ms\n", rowAnalyticsTime / 1_000_000.0);
            
            System.out.println("\nColumn Store - Only need 'country' and 'balance' columns:");
            long colAnalyticsStart = System.nanoTime();
            
            // With column store we can specify just the columns we need
            Map<String, List<Integer>> colCountryBalances = new HashMap<>();
            
            // Only request the columns we need for analysis
            List<String> neededColumns = Arrays.asList("country", "balance");
            List<Record> colRecords = columnStore.scan(startKey, endKey, neededColumns);
            
            for (Record record : colRecords) {
                Map<String, Object> data = record.getValue();
                String country = (String) data.get("country");
                Integer balance = (Integer) data.get("balance");
                
                colCountryBalances.computeIfAbsent(country, k -> new ArrayList<>()).add(balance);
            }
            
            // Calculate and display averages
            for (Map.Entry<String, List<Integer>> entry : colCountryBalances.entrySet()) {
                String country = entry.getKey();
                List<Integer> balances = entry.getValue();
                double average = balances.stream().mapToDouble(Integer::intValue).average().orElse(0.0);
                System.out.printf("Country: %s, Average Balance: $%.2f (from %d customers)\n", 
                        country, average, balances.size());
            }
            
            long colAnalyticsTime = System.nanoTime() - colAnalyticsStart;
            System.out.printf("Column Store analytics time: %.3f ms\n", colAnalyticsTime / 1_000_000.0);
            
            System.out.println("\nColumn Store Advantage: For analytics queries that only need a subset of columns,");
            System.out.println("column-based storage is typically faster because:");
            System.out.println("1. It only reads the columns needed (reduced I/O)");
            System.out.println("2. Better data compression (similar values together)");
            System.out.println("3. More efficient CPU cache usage (better vectorization)");
            
            // Close the storage engines
            rowStore.close();
            columnStore.close();
            
        } catch (Exception e) {
            System.err.println("Error in storage comparison: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Clean up
            deleteDirectory(tempDir);
        }
    }
    
    /**
     * Generates sample customer data.
     */
    private static List<Record> generateSampleCustomerData(int count) {
        List<Record> records = new ArrayList<>(count);
        Random random = new Random(42); // Fixed seed for reproducibility
        String[] countries = {"US", "UK", "DE", "FR", "JP", "IN", "BR", "CA", "AU", "IT"};
        
        for (int i = 0; i < count; i++) {
            // Use i as the customer ID
            byte[] key = ByteBuffer.allocate(4).putInt(i).array();
            
            Map<String, Object> value = new HashMap<>();
            value.put("id", i);
            value.put("name", "Customer " + i);
            value.put("email", "customer" + i + "@example.com");
            value.put("age", 20 + random.nextInt(60)); // Age between 20-79
            
            // Assign country - make distribution uneven for more realistic analysis
            String country = countries[random.nextInt(10)];
            value.put("country", country);
            
            // Vary balance by country for more interesting analytics
            double baseBalance = 1000.0; 
            // US, UK customers tend to have higher balances in this example
            if (country.equals("US") || country.equals("UK")) {
                baseBalance = 5000.0;
            } else if (country.equals("JP") || country.equals("DE")) {
                baseBalance = 3000.0;
            }
            
            int balance = (int)(baseBalance + (random.nextDouble() * 10000));
            value.put("balance", balance);
            
            records.add(new Record(key, value));
        }
        
        return records;
    }
    
    /**
     * Helper method to recursively delete a directory.
     */
    private static void deleteDirectory(Path path) throws IOException {
        if (Files.exists(path)) {
            Files.walk(path)
                .sorted(Comparator.reverseOrder())
                .forEach(p -> {
                    try {
                        Files.delete(p);
                    } catch (IOException e) {
                        System.err.println("Failed to delete: " + p);
                    }
                });
        }
    }
}
