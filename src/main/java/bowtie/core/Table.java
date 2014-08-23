package bowtie.core;

import bowtie.core.exceptions.InvalidScanParametersException;

import java.io.IOException;

/**
 * Represents an underlying bowtie key-value store.
 */
public interface Table {

    /**
     * Indexes the given key/value pair, overwriting any existing value for the key.
     * @param key
     * @param value
     * @throws IOException
     */
    void put(byte[] key, byte[] value) throws IOException;

    /**
     * Deletes the existing value for the supplied key or does nothing if no such value exists.
     * @param key
     * @throws IOException
     */
    void delete(byte[] key) throws IOException;

    /**
     * Flushes the current contents of the mem-table onto disk.
     * @throws IOException
     */
    void flush() throws IOException;

    /**
     * Returns all results lexicographically between the supplied start key (inclusive) and end key (exclusive).
     * @param inclStart scan start (inclusive)
     * @param exclStop scan end (exclusive)
     * @return all results between start and end
     * @throws IOException
     * @throws InvalidScanParametersException if the given start key is not lexicographically prior to the given end key
     */
    Iterable<Result> scan(byte[] inclStart, byte[] exclStop) throws IOException, InvalidScanParametersException;

    /**
     * Returns the result stored for the given key; if no value exists for the given key, returns a result with a null value (where noVal() == true).
     * @param key
     * @return
     * @throws IOException
     */
    Result get(byte[] key) throws IOException;

    /**
     * Opens this table for reads and writes.
     * @throws IOException
     */
    void open() throws IOException;

    /**
     * Returns whether the table is open for reads and writes.
     * @return
     */
    boolean isOpen();

    /**
     * Flushes the table and closes it for writes.
     * @throws IOException
     */
    void close() throws IOException;

    /**
     * Returns whether the table exists.
     * @return
     */
    boolean exists();

    /**
     * Initializes this table by creating a mem-table and data storage directory.
     * @throws IOException
     */
    void create() throws IOException;

    /**
     * Drops the table.
     * @throws IOException
     */
    void drop() throws IOException;

    /**
     * Returns the name of this table.
     * @return
     */
    String getName();

    /**
     * Performs a minor compaction on this table, which compacts all data files that have never been compacted.
     * @throws IOException
     */
    void compactMinor() throws IOException;

    /**
     * Performs a major compaction on this table, which compacts all data files.
     * @throws IOException
     */
    void compactMajor() throws IOException;
}
