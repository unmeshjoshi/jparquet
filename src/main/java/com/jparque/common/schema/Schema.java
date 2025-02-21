package com.jparque.schema;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents the schema of a dataset, containing column names and their corresponding data types.
 */
public class Schema {
    private final Map<String, DataType> columns;

    public Schema() {
        this.columns = new HashMap<>();
    }

    /**
     * Adds a new column to the schema with the specified name and data type.
     *
     * @param name The name of the column
     * @param dataType The data type of the column
     * @throws IllegalArgumentException if the column name already exists
     */
    public void addColumn(String name, DataType dataType) {
        if (columns.containsKey(name)) {
            throw new IllegalArgumentException("Column '" + name + "' already exists in the schema");
        }
        columns.put(name, dataType);
    }

    /**
     * Gets the data type of a column by its name.
     *
     * @param name The name of the column
     * @return The data type of the column
     * @throws IllegalArgumentException if the column does not exist
     */
    public DataType getDataType(String name) {
        DataType dataType = columns.get(name);
        if (dataType == null) {
            throw new IllegalArgumentException("Column '" + name + "' does not exist in the schema");
        }
        return dataType;
    }
}
