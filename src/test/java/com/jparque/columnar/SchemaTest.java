package com.jparque.columnar;

import com.jparque.common.schema.MessageType;
import com.jparque.common.schema.OriginalType;
import com.jparque.common.schema.Repetition;
import com.jparque.common.schema.Type;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MessageTypeTest {
    @Test
    void shouldCreateMessageTypeWithFields() {
        MessageType.Builder builder = new MessageType.Builder("test")
            .addField("id", Type.INT32, Repetition.REQUIRED)
            .addField("name", Type.BINARY, Repetition.REQUIRED, OriginalType.UTF8)
            .addField("age", Type.INT32, Repetition.OPTIONAL)
            .addField("tags", Type.BINARY, Repetition.REPEATED, OriginalType.UTF8);

        MessageType messageType = builder.build();

        assertThat(messageType.getName()).isEqualTo("test");
        assertThat(messageType.getField("id").getType()).isEqualTo(Type.INT32);
        assertThat(messageType.getField("name").getOriginalType()).isEqualTo(OriginalType.UTF8);
        assertThat(messageType.getField("age").getRepetition()).isEqualTo(Repetition.OPTIONAL);
        assertThat(messageType.getField("tags").getRepetition()).isEqualTo(Repetition.REPEATED);
    }

    @Test
    void shouldThrowExceptionForUnknownField() {
        MessageType messageType = new MessageType.Builder("test")
            .addField("id", Type.INT32, Repetition.REQUIRED)
            .build();

        assertThrows(IllegalArgumentException.class, () -> {
            messageType.getField("unknown");
        });
    }

    @Test
    void shouldGenerateCorrectStringRepresentation() {
        MessageType messageType = new MessageType.Builder("Person")
            .addField("id", Type.INT64, Repetition.REQUIRED, null, 1)
            .addField("name", Type.BINARY, Repetition.REQUIRED, OriginalType.UTF8, 2)
            .build();

        String expected = "message Person {\n" +
                         "  required int64 id = 1;\n" +
                         "  required binary name (UTF8) = 2;\n" +
                         "}";

        assertThat(messageType.toString()).isEqualTo(expected);
    }
}
