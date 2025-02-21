package com.jparque.common.schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Represents a Parquet message type, which is the root of a Parquet schema.
 * This implementation aligns with Parquet's actual MessageType class.
 */
public class MessageType {
    private final String name;
    private final List<Field> fields;
    private final int version;
    private final MessageType previousVersion;

    public MessageType(String name, List<Field> fields) {
        this(name, fields, 1, null);
    }

    public MessageType(String name, List<Field> fields, int version, MessageType previousVersion) {
        this.name = Objects.requireNonNull(name, "name cannot be null");
        this.fields = new ArrayList<>(Objects.requireNonNull(fields, "fields cannot be null"));
        this.version = version;
        this.previousVersion = previousVersion;
    }

    public String getName() {
        return name;
    }

    public List<Field> getFields() {
        return Collections.unmodifiableList(fields);
    }

    public int getVersion() {
        return version;
    }

    public MessageType getPreviousVersion() {
        return previousVersion;
    }

    public Field getField(String fieldName) {
        for (Field field : fields) {
            if (field.getName().equals(fieldName)) {
                return field;
            }
        }
        throw new IllegalArgumentException("Field '" + fieldName + "' does not exist");
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("message ").append(name).append(" {\n");
        for (Field field : fields) {
            sb.append("  ").append(field.toString()).append(";\n");
        }
        sb.append("}");
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MessageType that = (MessageType) o;
        return name.equals(that.name) && fields.equals(that.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, fields);
    }

    /**
     * Builder class for creating MessageType instances.
     */
    public static class Builder {
        private final String name;
        private final List<Field> fields = new ArrayList<>();
        private MessageType previousVersion;
        private int version = 1;

        public Builder(String name) {
            this.name = Objects.requireNonNull(name, "name cannot be null");
        }

        public Builder addField(String name, Type type, Repetition repetition) {
            return addField(name, type, repetition, null, -1);
        }

        public Builder addField(String name, Type type, Repetition repetition, OriginalType originalType) {
            return addField(name, type, repetition, originalType, -1);
        }

        public Builder addField(String name, Type type, Repetition repetition, OriginalType originalType, int id) {
            fields.add(new Field(name, type, repetition, originalType, id));
            return this;
        }

        public Builder setPreviousVersion(MessageType previousVersion) {
            this.previousVersion = previousVersion;
            this.version = previousVersion != null ? previousVersion.getVersion() + 1 : 1;
            return this;
        }

        public MessageType build() {
            return new MessageType(name, fields, version, previousVersion);
        }
    }
}
