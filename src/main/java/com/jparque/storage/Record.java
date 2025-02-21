package com.jparque.storage;

import java.util.Map;

/**
 * Represents a record in the storage engine.
 */
public class Record {
    private final byte[] key;
    private final Map<String, Object> value;

    public Record(byte[] key, Map<String, Object> value) {
        this.key = key;
        this.value = value;
    }

    public byte[] getKey() {
        return key;
    }

    public Map<String, Object> getValue() {
        return value;
    }
}
