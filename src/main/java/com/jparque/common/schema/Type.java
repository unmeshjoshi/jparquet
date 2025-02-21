package com.jparque.common.schema;

/**
 * Represents the primitive types supported in Parquet.
 * This aligns with Parquet's actual type system.
 */
public enum Type {
    BOOLEAN(0),
    INT32(1),
    INT64(2),
    INT96(3),
    FLOAT(4),
    DOUBLE(5),
    BINARY(6),
    FIXED_LEN_BYTE_ARRAY(7);

    private final int value;

    Type(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static Type fromValue(int value) {
        for (Type type : values()) {
            if (type.value == value) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown type value: " + value);
    }
}
