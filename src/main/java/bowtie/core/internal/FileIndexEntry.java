package bowtie.core.internal;

import bowtie.core.internal.util.ByteUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Map;
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
    private static final int TIMESTAMP_LENGTH = 8;
    private static final int POSITION_LENGTH = 8;
    private static final int NUM_KEY_POSITION_DESCRIPTOR_LENGTH = 4;
    private static final int TOTAL_LENGTH_DESCRIPTOR_LENGTH = 4;

    private byte[] startKey;
    private byte[] endKey;
    private final String fileName;
    private final NavigableMap<byte[], Long> keyPositions;
    private final Long fileTimestamp;
    private int bytesLength;

    public FileIndexEntry(Long fileTimestamp) {
        this.keyPositions = new TreeMap<byte[], Long>(ByteUtils.COMPARATOR);
        this.fileTimestamp = fileTimestamp;
        this.fileName = fileTimestamp.toString();
        bytesLength = TOTAL_LENGTH_DESCRIPTOR_LENGTH + TIMESTAMP_LENGTH + NUM_KEY_POSITION_DESCRIPTOR_LENGTH;
    }

    public void setStartKey(byte[] startKey) {
        this.startKey = startKey;
        bytesLength += ByteUtils.SIZE_DESCRIPTOR_LENGTH + startKey.length;
        addIndexedKey(startKey, 0);
    }

    public void setEndKey(byte[] endKey) {
        this.endKey = endKey;
        bytesLength += ByteUtils.SIZE_DESCRIPTOR_LENGTH + endKey.length;
    }

    public void addIndexedKey(byte[] key, long position) {
        keyPositions.put(key, position);
        bytesLength += ByteUtils.SIZE_DESCRIPTOR_LENGTH + key.length + POSITION_LENGTH;
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

    public byte[] toBytes() {
        final ByteBuffer buf = ByteBuffer.allocate(bytesLength);
        buf.putInt(bytesLength);
        buf.putLong(fileTimestamp);
        buf.putShort((short) startKey.length);
        buf.put(startKey);
        buf.putShort((short) endKey.length);
        buf.put(endKey);
        buf.putInt(keyPositions.size());
        for (final Map.Entry<byte[], Long> keyAndPosition : keyPositions.entrySet()) {
            buf.putShort((short) keyAndPosition.getKey().length);
            buf.put(keyAndPosition.getKey());
            buf.putLong(keyAndPosition.getValue());
        }
        return buf.array();
    }

    public static FileIndexEntry fromBytes(final InputStream inputStream) throws IOException {
        // read size
        final byte[] lengthBytes = new byte[TOTAL_LENGTH_DESCRIPTOR_LENGTH];
        inputStream.read(lengthBytes);
        final ByteBuffer lengthReader = ByteBuffer.wrap(lengthBytes);
        final int size = lengthReader.getInt();

        // read rest of bytes
        final byte[] restOfBytes = new byte[size - TOTAL_LENGTH_DESCRIPTOR_LENGTH];
        inputStream.read(restOfBytes);
        final ByteBuffer reader = ByteBuffer.wrap(restOfBytes);
        final long timestamp = reader.getLong();
        final FileIndexEntry fileIndexEntry = new FileIndexEntry(timestamp);
        final short startKeyLength = reader.getShort();
        final byte[] startKey = new byte[startKeyLength];
        reader.get(startKey);
        fileIndexEntry.setStartKey(startKey);
        final short endKeyLength = reader.getShort();
        final byte[] endKey = new byte[endKeyLength];
        reader.get(endKey);
        fileIndexEntry.setEndKey(endKey);
        final int numKeyPositionEntries = reader.getInt();
        for (int i=0; i<numKeyPositionEntries; i++) {
            final short keyLength = reader.getShort();
            final byte[] key = new byte[keyLength];
            reader.get(key);
            final long position = reader.getLong();
            fileIndexEntry.addIndexedKey(key, position);
        }
        return fileIndexEntry;
    }
}
