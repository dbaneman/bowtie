package bowtie.core.impl;

import bowtie.core.api.internal.IFileIndexEntry;

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
    public void addIndexedKey(byte[] key, int position) {
        // TODO
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
}
