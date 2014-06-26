package bowtie.core.internal;

import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 2/8/14
 * Time: 6:23 PM
 * To change this template use File | Settings | File Templates.
 */
public class FileIndexEntry {
    private byte[] startKey;
    private byte[] endKey;
    private String fileName;
    private final NavigableMap<byte[], Long> keyPositions;

    public FileIndexEntry() {
        this.keyPositions = new TreeMap<byte[], Long>();
    }

    public void setStartKey(byte[] startKey) {
        this.startKey = startKey;
    }

    public void setEndKey(byte[] endKey) {
        this.endKey = endKey;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public void addIndexedKey(byte[] key, long position) {
        keyPositions.put(key, position);
    }

    public byte[] getStartKey() {
        return startKey;
    }

    public byte[] getEndKey() {
        return endKey;
    }

    public String getFileName() {
        return fileName;
    }

    public NavigableMap<byte[], Long> getKeyPositions() {
        return keyPositions;
    }

}
