package bowtie.core.internal;

import bowtie.core.internal.util.ByteUtils;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 2/8/14
 * Time: 6:14 PM
 */
public class Index {
    private final NavigableMap<byte[], List<Entry>> index;
    private final OutputStream indexFileWriter;

    public Index(final String indexFileAbsolutePath) throws IOException {
        final File indexFile = new File(indexFileAbsolutePath);
        if (indexFile.exists()) {
            final InputStream indexFileReader = new BufferedInputStream(new FileInputStream(indexFile));
            index = fromBytes(indexFileReader);
            indexFileReader.close();
        } else {
            index = newMap();
        }
        this.indexFileWriter = new BufferedOutputStream(FileUtils.openOutputStream(indexFile, true));
    }

    private static NavigableMap<byte[], List<Entry>> newMap() {
        return new TreeMap<byte[], List<Entry>>(ByteUtils.COMPARATOR);
    }

    private static NavigableMap<byte[], List<Entry>> fromBytes(final InputStream inputStream) throws IOException {
        final NavigableMap<byte[], List<Entry>> ret = newMap();
        int nextByte;
        while ((nextByte=inputStream.read()) != -1) {
            final Entry entry = Entry.fromBytes(inputStream);
            addEntryToMap(ret, entry);
        }
        return ret;
    }

    public void addEntry(Entry entry) throws IOException {
        // persist index entry to disk
        indexFileWriter.write(1);
        indexFileWriter.write(entry.toBytes());
        indexFileWriter.flush();

        // add to in-memory index
        addEntryToMap(index, entry);
    }

    private static void addEntryToMap(final NavigableMap<byte[], List<Entry>> map, final Entry entry) {
        List<Entry> entries = map.get(entry.getStartKey());
        if (entries == null) {
            entries = new ArrayList<Entry>(1);
        }
        entries.add(entry);
        map.put(entry.getStartKey(), entries);
    }

    /**
     * Return all files possibly containing any entries in the specified key ranges. This means all files whose start key
     * is less than exclEnd AND whose end key is greater than or equal to inclStart.
     */
    public List<Entry> getFilesPossiblyContainingKeyRange(byte[] inclStart, byte[] exclEnd) {
        List<Entry> ret = new ArrayList<Entry>();
        final Map<byte[], List<Entry>> headMap = exclEnd==null
                ? index.headMap(inclStart, true)
                : index.headMap(exclEnd, false);
        for (final Map.Entry<byte[], List<Entry>> startKeyAndEntryList : headMap.entrySet()) {
            for (final Entry entry : startKeyAndEntryList.getValue()) {
                if (exclEnd==null || ByteUtils.compare(entry.getEndKey(), inclStart) >= 0) {
                    ret.add(entry);
                }
            }
        }
        return ret;
    }

    /**
     * Get the file position to start scanning for the specified key range. If the file's start key is less than or equal
     * to the scan's start key, return the latest indexed position in the file that's before or at the start key.
     * Otherwise (if the file's start key is greater than the scan's start key), simply return the file's start key (0).
     */
    public long getStartingIndexInFileForScan(byte[] inclStart, Entry possibleHit) {
        return ByteUtils.compare(possibleHit.getStartKey(), inclStart) <= 0
                ? possibleHit.getKeyPositions().headMap(inclStart, true).lastEntry().getValue()
                : possibleHit.getKeyPositions().firstEntry().getValue();
    }

    public void close() throws IOException {
        indexFileWriter.close();
    }


    public static class Entry {
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

        public Entry(Long fileTimestamp) {
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

        public static Entry fromBytes(final InputStream inputStream) throws IOException {
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
            final Entry entry = new Entry(timestamp);
            final short startKeyLength = reader.getShort();
            final byte[] startKey = new byte[startKeyLength];
            reader.get(startKey);
            entry.setStartKey(startKey);
            final short endKeyLength = reader.getShort();
            final byte[] endKey = new byte[endKeyLength];
            reader.get(endKey);
            entry.setEndKey(endKey);
            final int numKeyPositionEntries = reader.getInt();
            for (int i=0; i<numKeyPositionEntries; i++) {
                final short keyLength = reader.getShort();
                final byte[] key = new byte[keyLength];
                reader.get(key);
                final long position = reader.getLong();
                entry.addIndexedKey(key, position);
            }
            return entry;
        }
    }
}
