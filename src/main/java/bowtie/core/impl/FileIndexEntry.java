package bowtie.core.impl;

import bowtie.core.api.internal.IFileIndexEntry;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 2/8/14
 * Time: 6:23 PM
 * To change this template use File | Settings | File Templates.
 */
public class FileIndexEntry implements IFileIndexEntry {
    private byte[] startKey;
    private byte[] endKey;
    private String fileName;
    private final List<Map.Entry<byte[], Long>> keyPositions;

    public FileIndexEntry() {
        this.keyPositions = new ArrayList<Map.Entry<byte[], Long>>();
    }

    @Override
    public void setStartKey(byte[] startKey) {
        this.startKey = startKey;
    }

    @Override
    public void setEndKey(byte[] endKey) {
        this.endKey = endKey;
    }

    @Override
    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    @Override
    public void addIndexedKey(byte[] key, long position) {
        keyPositions.add(new AbstractMap.SimpleEntry<byte[], Long>(key, position));
    }

    @Override
    public byte[] getStartKey() {
        return startKey;
    }

    @Override
    public byte[] getEndKey() {
        return endKey;
    }

    @Override
    public String getFileName() {
        return fileName;
    }

    public List<Map.Entry<byte[], Long>> getKeyPositions() {
        return keyPositions;
    }

}
