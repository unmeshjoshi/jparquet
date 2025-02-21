package com.jparque.common.schema;

import java.util.Objects;
import com.jparque.common.schema.Type;
import com.jparque.common.schema.OriginalType;
import com.jparque.common.schema.Repetition;

/**
 * Represents a field in a Parquet schema.
 * This aligns with Parquet's actual implementation where fields can be required, optional, or repeated.
 */
public class Field {
    private final String name;
    private final Type type;
    private final OriginalType originalType;
    private final Repetition repetition;
    private final int id;

    public Field(String name, Type type, Repetition repetition, OriginalType originalType, int id) {
        this.name = Objects.requireNonNull(name, "name cannot be null");
        this.type = Objects.requireNonNull(type, "type cannot be null");
        this.repetition = Objects.requireNonNull(repetition, "repetition cannot be null");
        this.originalType = originalType; // originalType can be null
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public Type getType() {
        return type;
    }

    public OriginalType getOriginalType() {
        return originalType;
    }

    public Repetition getRepetition() {
        return repetition;
    }

    public int getId() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Field field = (Field) o;
        return id == field.id &&
                name.equals(field.name) &&
                type == field.type &&
                repetition == field.repetition &&
                Objects.equals(originalType, field.originalType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, originalType, repetition, id);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(repetition.toString().toLowerCase())
          .append(" ")
          .append(type.toString().toLowerCase())
          .append(" ")
          .append(name);
        
        if (originalType != null) {
            sb.append(" (").append(originalType.toString()).append(")");
        }
        
        if (id >= 0) {
            sb.append(" = ").append(id);
        }
        
        return sb.toString();
    }
}
