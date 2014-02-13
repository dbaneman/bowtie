package bowtie.core.api.internal;

import java.util.NavigableMap;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 2/8/14
 * Time: 6:17 PM
 * To change this template use File | Settings | File Templates.
 */
public interface IFileIndexEntry {
    void setStartKey(byte[] startKey);
    void setEndKey(byte[] endKey);
    void setFileName(String fileName);
    void addIndexedKey(byte[] key, long position);
    byte[] getStartKey();
    byte[] getEndKey();
    String getFileName();
    NavigableMap<byte[], Long> getKeyPositions();
}
