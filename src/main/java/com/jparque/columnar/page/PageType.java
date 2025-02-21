package com.jparque.columnar.page;

/**
 * Represents the different types of pages in a Parquet file.
 */
public enum PageType {
    DATA_PAGE(0),
    INDEX_PAGE(1),
    DICTIONARY_PAGE(2),
    DATA_PAGE_V2(3);

    private final int value;

    PageType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static PageType fromValue(int value) {
        for (PageType type : values()) {
            if (type.value == value) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown page type value: " + value);
    }
}
