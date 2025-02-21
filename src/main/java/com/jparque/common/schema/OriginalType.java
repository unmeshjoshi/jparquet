package com.jparque.common.schema;

/**
 * Represents the original (logical) types in Parquet.
 * These are used to annotate primitive types with additional semantic information.
 */
public enum OriginalType {
    UTF8(0),            // Used with BINARY for string data
    MAP(1),             // Used for map structures
    LIST(2),            // Used for list structures
    DECIMAL(3),         // Used with BINARY or FIXED_LEN_BYTE_ARRAY for decimal numbers
    DATE(4),            // Used with INT32 for date values
    TIME_MILLIS(5),     // Used with INT32 for time values
    TIMESTAMP_MILLIS(6),// Used with INT64 for timestamp values
    INTERVAL(7);        // Used with FIXED_LEN_BYTE_ARRAY for interval values

    private final int value;

    OriginalType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static OriginalType fromValue(int value) {
        for (OriginalType type : values()) {
            if (type.value == value) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown original type value: " + value);
    }
}
