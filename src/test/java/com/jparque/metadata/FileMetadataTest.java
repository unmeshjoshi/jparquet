package com.jparque.metadata;

import com.jparque.columnar.chunk.ColumnChunk;
import com.jparque.columnar.chunk.CompressionCodec;
import com.jparque.columnar.page.DataPage;
import com.jparque.columnar.page.Encoding;
import com.jparque.columnar.page.Statistics;
import com.jparque.columnar.rowgroup.RowGroup;
import com.jparque.common.FileMetadata;
import com.jparque.common.ParquetConstants;
import com.jparque.common.schema.Field;
import com.jparque.common.schema.MessageType;
import com.jparque.common.schema.OriginalType;
import com.jparque.common.schema.Repetition;
import com.jparque.common.schema.Type;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

class FileMetadataTest {
    @Test
    void shouldWriteFileMetadataCorrectly() throws Exception {
        // Create schema
        MessageType schema = new MessageType.Builder("Person")
            .addField("age", Type.INT32, Repetition.REQUIRED)
            .addField("name", Type.BINARY, Repetition.REQUIRED, OriginalType.UTF8)
            .build();

        // Create test data for INT32 column
        byte[] intData = new byte[] { 1, 0, 0, 0 }; // 1 as a 4-byte integer
        ByteBuffer intBuffer = ByteBuffer.wrap(intData);
        Statistics intStats = new Statistics(
            ByteBuffer.wrap(new byte[] { 1, 0, 0, 0 }), // min = 1
            ByteBuffer.wrap(new byte[] { 1, 0, 0, 0 }), // max = 1
            0, // null count
            1  // distinct count
        );
        DataPage intPage = new DataPage(
            intBuffer,
            1,
            Encoding.PLAIN,
            Encoding.RLE,
            Encoding.BIT_PACKED,
            intStats
        );
        ColumnChunk intColumn = new ColumnChunk.Builder()
            .setType(Type.INT32)
            .setCodec(CompressionCodec.UNCOMPRESSED)
            .setPath("age")
            .setValueCount(1)
            .setTotalSize(24 + intPage.getHeaderSize() + intPage.getData().remaining())
            .setFirstDataPageOffset(0)
            .addPage(intPage)
            .build();

        // Create test data for BINARY column
        byte[] strData = "test".getBytes();
        ByteBuffer strBuffer = ByteBuffer.wrap(strData);
        Statistics strStats = new Statistics(
            ByteBuffer.wrap("test".getBytes()),
            ByteBuffer.wrap("test".getBytes()),
            0,
            1
        );
        DataPage strPage = new DataPage(
            strBuffer,
            1,
            Encoding.PLAIN,
            Encoding.RLE,
            Encoding.BIT_PACKED,
            strStats
        );
        ColumnChunk strColumn = new ColumnChunk.Builder()
            .setType(Type.BINARY)
            .setCodec(CompressionCodec.UNCOMPRESSED)
            .setPath("name")
            .setValueCount(1)
            .setTotalSize(24 + strPage.getHeaderSize() + strPage.getData().remaining())
            .setFirstDataPageOffset(0)
            .addPage(strPage)
            .build();

        // Create row group
        RowGroup rowGroup = new RowGroup.Builder()
            .addColumn(intColumn)
            .addColumn(strColumn)
            .setRowCount(1)
            .build();

        // Create file metadata
        FileMetadata metadata = new FileMetadata.Builder()
            .setSchema(schema)
            .setVersion(1)
            .setCreatedBy("JParque Test")
            .addRowGroup(rowGroup)
            .build();

        // Create a temporary file for testing
        File tempFile = File.createTempFile("metadata-test", ".parquet");
        tempFile.deleteOnExit();

        // Write metadata to file
        try (RandomAccessFile file = new RandomAccessFile(tempFile, "rw");
             FileChannel channel = file.getChannel()) {
            
            // Write row group first
            long dataLength = rowGroup.writeTo(channel);
            
            // Write metadata at the end
            long metadataStart = channel.position();
            long metadataLength = metadata.writeTo(channel);

            // Write footer
            ByteBuffer footer = ByteBuffer.allocate(8);
            footer.putInt((int) metadataLength);
            footer.putInt(ParquetConstants.FOOTER_MAGIC);
            footer.flip();
            channel.write(footer);

            // Verify file structure
            channel.position(0);
            ByteBuffer readBuffer = ByteBuffer.allocate((int) channel.size());
            channel.read(readBuffer);
            readBuffer.flip();

            // Skip row group data
            readBuffer.position((int) dataLength);

            // Verify metadata
            // Version
            assertThat(readBuffer.getInt()).isEqualTo(1);

            // Schema
            assertThat(readBuffer.getInt()).isEqualTo(schema.getFields().size()); // number of fields
            for (Field field : schema.getFields()) {
                assertThat(readBuffer.getInt()).isEqualTo(field.getType().getValue());
                assertThat(readBuffer.getInt()).isEqualTo(field.getRepetition().getValue());
                if (field.getOriginalType() != null) {
                    assertThat(readBuffer.getInt()).isEqualTo(field.getOriginalType().getValue());
                }
                byte[] nameBytes = new byte[readBuffer.getInt()];
                readBuffer.get(nameBytes);
                assertThat(new String(nameBytes)).isEqualTo(field.getName());
            }

            // Created by
            byte[] createdByBytes = new byte[readBuffer.getInt()];
            readBuffer.get(createdByBytes);
            assertThat(new String(createdByBytes)).isEqualTo("JParque Test");

            // Row group metadata
            assertThat(readBuffer.getLong()).isEqualTo(1); // num row groups
            assertThat(readBuffer.getLong()).isEqualTo(rowGroup.getRowCount());
            assertThat(readBuffer.getLong()).isEqualTo(rowGroup.getTotalByteSize());
            assertThat(readBuffer.getLong()).isEqualTo(0); // start offset

            // Footer
            assertThat(readBuffer.getInt()).isEqualTo((int) metadataLength);
            assertThat(readBuffer.getInt()).isEqualTo(ParquetConstants.FOOTER_MAGIC);
        }
    }
}
