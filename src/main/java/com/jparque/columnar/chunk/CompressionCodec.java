package com.jparque.columnar.chunk;

/**
 * Represents the different compression codecs available in Parquet.
 */
public enum CompressionCodec {
    UNCOMPRESSED(0),
    SNAPPY(1),
    GZIP(2),
    LZO(3),
    BROTLI(4),
    LZ4(5),
    ZSTD(6);

    private final int value;

    CompressionCodec(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static CompressionCodec fromValue(int value) {
        for (CompressionCodec codec : values()) {
            if (codec.value == value) {
                return codec;
            }
        }
        throw new IllegalArgumentException("Unknown compression codec value: " + value);
    }
}
