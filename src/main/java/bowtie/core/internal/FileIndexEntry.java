package bowtie.core.internal;

import bowtie.core.internal.util.ByteUtils;

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
    private final String fileName;
    private final NavigableMap<byte[], Long> keyPositions;
    private final Long fileTimestamp;

    public FileIndexEntry(Long fileTimestamp) {
        this.keyPositions = new TreeMap<byte[], Long>(ByteUtils.COMPARATOR);
        this.fileTimestamp = fileTimestamp;
        this.fileName = fileTimestamp.toString();
    }

    public void setStartKey(byte[] startKey) {
        this.startKey = startKey;
        addIndexedKey(startKey, 0);
    }

    public void setEndKey(byte[] endKey) {
        this.endKey = endKey;
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

    public long getFileTimestamp() {
        return fileTimestamp;
    }
}
