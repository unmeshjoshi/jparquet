package com.jparque.common.schema;

/**
 * Represents the repetition type of a field in Parquet schema.
 */
public enum Repetition {
    /**
     * Field must appear exactly once
     */
    REQUIRED(0),
    
    /**
     * Field can appear at most once (0 or 1 times)
     */
    OPTIONAL(1),
    
    /**
     * Field can appear 0 or more times
     */
    REPEATED(2);

    private final int value;

    Repetition(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static Repetition fromValue(int value) {
        for (Repetition repetition : values()) {
            if (repetition.value == value) {
                return repetition;
            }
        }
        throw new IllegalArgumentException("Unknown repetition value: " + value);
    }
}
