package bowtie.core;

import java.util.Map;

/**
 * Result object containing the key-value pair returned by a table get or scan.
 */
public interface Result extends Map.Entry<byte[], byte[]> {

    /**
     * Returns the key component of this result's key/value pair.
     * @return
     */
    @Override
    byte[] getKey();

    /**
     * Returns the value component of this result's key/value pair; returns null if no value exists for the given key.
     * @return
     */
    @Override
    byte[] getValue();

    /**
     * Returns whether this result has no value, which represents a key that has never been indexed or has been deleted.
     * @return
     */
    boolean noVal();
}
