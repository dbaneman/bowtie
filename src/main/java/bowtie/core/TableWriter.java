package bowtie.core;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 2/10/14
 * Time: 11:01 PM
 * To change this template use File | Settings | File Templates.
 */
public interface TableWriter {
    /**
     * Indexes the given key/value pair. If the key is already present, the existing value gets overwritten.
     */
    void put(byte[] key, byte[] value) throws IOException;

    void delete(byte[] key) throws IOException;

    void flush() throws IOException;
}