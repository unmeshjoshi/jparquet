package com.jparque.common;

/**
 * Constants used in the Parquet file format.
 */
public class ParquetConstants {
    /**
     * Magic number that appears at the end of every Parquet file.
     * Spelled out PAR1 in ASCII.
     */
    public static final int FOOTER_MAGIC = 0x50415231; // "PAR1" in ASCII

    private ParquetConstants() {
        // Prevent instantiation
    }
}
