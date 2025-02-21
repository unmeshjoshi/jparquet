package com.jparque.columnar.page;

/**
 * Represents the different encoding types available in Parquet.
 */
public enum Encoding {
    PLAIN(0),
    DICTIONARY(2),
    RLE(3),
    BIT_PACKED(4),
    DELTA_BINARY_PACKED(5),
    DELTA_LENGTH_BYTE_ARRAY(6),
    DELTA_BYTE_ARRAY(7),
    RLE_DICTIONARY(8);

    private final int value;

    Encoding(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static Encoding fromValue(int value) {
        for (Encoding encoding : values()) {
            if (encoding.value == value) {
                return encoding;
            }
        }
        throw new IllegalArgumentException("Unknown encoding value: " + value);
    }
}
