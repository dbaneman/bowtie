package bowtie.core.internal;

import bowtie.core.Result;
import bowtie.core.internal.util.ByteUtils;
import bowtie.core.internal.util.DataWriterUtil;
import bowtie.core.internal.util.MergedIterable;
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
    private final NavigableMap<byte[], List<Entry>> map;
    private final Set<byte[]> uncompactedEntries;
    private final OutputStream indexFileWriter;
    private final String filePath;
    private final String tableName;
    private final Conf conf;

    public String getName() {
        return tableName;
    }

    public Conf getConf() {
        return conf;
    }

    public static Index forFile(final Conf conf, final String tableName, final String indexFileAbsolutePath) throws IOException {
        File indexFile = new File(indexFileAbsolutePath);
        if (!indexFile.exists()) {
            indexFile = new File(indexFileAbsolutePath + ".bak"); // in case we hit an error while rewriting the index
        }
        return indexFile.exists()
                ? fromFile(conf, tableName, indexFile)
                : new Index(conf, tableName, newMap(), newSet(), indexFile, indexFileAbsolutePath);
    }

    private Index(final Conf conf, final String tableName, final NavigableMap<byte[], List<Entry>> map, final Set<byte[]> uncompactedEntries, final File indexFile, final String filePath) throws IOException {
        this.conf = conf;
        this.tableName = tableName;
        this.map = map;
        this.uncompactedEntries = uncompactedEntries;
        this.indexFileWriter = new BufferedOutputStream(FileUtils.openOutputStream(indexFile, true));
        this.filePath = filePath;
    }

    private static NavigableMap<byte[], List<Entry>> newMap() {
        return new TreeMap<byte[], List<Entry>>(ByteUtils.COMPARATOR);
    }

    private static Set<byte[]> newSet() {
        return new TreeSet<byte[]>(ByteUtils.COMPARATOR);
    }

    private static Index fromFile(final Conf conf, final String tableName, final File indexFile) throws IOException {
        final InputStream inputStream = new BufferedInputStream(new FileInputStream(indexFile));
        final NavigableMap<byte[], List<Entry>> map= newMap();
        final Set<byte[]> uncompactedEntries = newSet();
        while (inputStream.read() != -1) {
            final Entry entry = Entry.fromBytes(inputStream);
            addEntryToMap(map, uncompactedEntries, entry);
        }
        inputStream.close();
        return new Index(conf, tableName, map, uncompactedEntries, indexFile, indexFile.getAbsolutePath());
    }

    public void addEntry(Entry entry) throws IOException {
        // persist index entry to disk
        writeEntryToFile(indexFileWriter, entry);

        // add to in-memory index
        addEntryToMap(map, uncompactedEntries, entry);
    }

    private static void writeEntryToFile(final OutputStream indexFileWriter, final Entry entry) throws IOException {
        indexFileWriter.write(1);
        indexFileWriter.write(entry.toBytes());
        indexFileWriter.flush();
    }

    private static void addEntryToMap(final NavigableMap<byte[], List<Entry>> map, final Set<byte[]> uncompactedEntries, final Entry entry) {
        List<Entry> entries = map.get(entry.getStartKey());
        if (entries == null) {
            entries = new ArrayList<Entry>(1);
        }
        entries.add(entry);
        map.put(entry.getStartKey(), entries);
        if (entry.hasBeenCompacted()) {
            uncompactedEntries.add(entry.getStartKey());
        }
    }

    /**
     * Return all files possibly containing any entries in the specified key ranges. This means all files whose start key
     * is less than exclEnd AND whose end key is greater than or equal to inclStart.
     */
    public List<Entry> getFilesPossiblyContainingKeyRange(byte[] inclStart, byte[] exclEnd) {
        List<Entry> ret = new ArrayList<Entry>();
        final Map<byte[], List<Entry>> headMap = exclEnd==null
                ? map.headMap(inclStart, true)
                : map.headMap(exclEnd, false);
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

    public void compactMinor() throws IOException {
        final List<Entry> entriesToCompact = new ArrayList<Entry>();
        for (final byte[] keyWithUncompactedEntries : uncompactedEntries) {
            final List<Entry> entries = map.get(keyWithUncompactedEntries);
            final List<Entry> entriesToRemove = new ArrayList<Entry>();
            for (final Entry possiblyUncompactedEntry : entries) {
                if (!possiblyUncompactedEntry.hasBeenCompacted()) {
                    entriesToRemove.add(possiblyUncompactedEntry);
                    entriesToCompact.add(possiblyUncompactedEntry);
                }
            }
            entries.removeAll(entriesToRemove);
            if (entries.isEmpty()) {
                map.remove(keyWithUncompactedEntries);
            } else {
                map.put(keyWithUncompactedEntries, entries);
            }
        }
        final List<Entry> compactedEntries = compact(entriesToCompact);
        for (final Entry entry : compactedEntries) {
            addEntryToMap(map, uncompactedEntries, entry);
        }
        rewriteIndexFile(getAllEntries());
    }

    public void rewriteIndexFile(final List<Entry> entries) throws IOException {
        final File tempOutputFile = new File(filePath + ".compacting");
        final OutputStream outputStream = new BufferedOutputStream(FileUtils.openOutputStream(tempOutputFile, false));
        for (final Entry entry : entries) {
            writeEntryToFile(outputStream, entry);
        }
        outputStream.close();
        FileUtils.moveFile(new File(filePath), new File(filePath + ".bak"));
        FileUtils.moveFile(tempOutputFile, new File(filePath));
        FileUtils.forceDelete(new File(filePath + ".bak"));
    }

    public List<Entry> getAllEntries() {
        final List<Entry> ret = new ArrayList<Entry>();
        for (final List<Entry> entryList : map.values()) {
            for (final Entry entry : entryList) {
                ret.add(entry);
            }
        }
        return ret;
    }

    /**
     * Find the data files associated with the provided index  entries. Merge the data files into a set of new (fewer,
     * larger) files with no overlapping keys. Then delete the original data files. Return the index entries for the new
     * data files.
     */
    public List<Entry> compact(final List<Entry> inputEntries) throws IOException {
        final List<Iterable<Result>> resultIterables = new ArrayList<Iterable<Result>>();
        for (final Entry inputEntry : inputEntries) {
            resultIterables.add(ResultIterator.asIterable(inputEntry.getFileName(), inputEntry.getFileTimestamp()));
        }
        final MergedIterable<Result> mergedIterable = new MergedIterable<Result>(ResultImpl.KEY_BASED_RESULT_COMPARATOR, resultIterables, true);
        return DataWriterUtil.writeDataFilesAndCreateIndexEntries(getConf(), getName(), mergedIterable);
    }

    public String getFilePath() {
        return filePath;
    }

    public static class Entry {
        private static final int TIMESTAMP_LENGTH = 8;
        private static final int POSITION_LENGTH = 8;
        private static final int NUM_KEY_POSITION_DESCRIPTOR_LENGTH = 4;
        private static final int TOTAL_LENGTH_DESCRIPTOR_LENGTH = 4;
        private static final int COMPACTED_LENGTH = 1;
        private static final byte TRUE = 1;
        private static final byte FALSE = 0;

        private byte[] startKey;
        private byte[] endKey;
        private final String fileName;
        private final NavigableMap<byte[], Long> keyPositions;
        private final Long fileTimestamp;
        private final boolean compacted;
        private int bytesLength;

        public Entry(Long fileTimestamp, boolean compacted) {
            this.keyPositions = new TreeMap<byte[], Long>(ByteUtils.COMPARATOR);
            this.fileTimestamp = fileTimestamp;
            this.fileName = fileTimestamp.toString();
            this.compacted = compacted;
            bytesLength = TOTAL_LENGTH_DESCRIPTOR_LENGTH + TIMESTAMP_LENGTH + NUM_KEY_POSITION_DESCRIPTOR_LENGTH + COMPACTED_LENGTH;
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
            buf.put(compacted ? TRUE : FALSE);
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
            final boolean compacted;
            switch(reader.get()) {
                case FALSE :
                    compacted = false;
                    break;
                case TRUE :
                    compacted = true;
                    break;
                default :
                    throw new RuntimeException();
            }
            final Entry entry = new Entry(timestamp, compacted);
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

        public boolean hasBeenCompacted() {
            return compacted;
        }
    }
}
