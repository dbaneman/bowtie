package bowtie.core.internal;

import bowtie.core.internal.util.ByteUtils;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 2/8/14
 * Time: 6:14 PM
 */
public class FileIndex {
    private final NavigableMap<byte[], List<FileIndexEntry>> index;
    private final OutputStream indexFileWriter;

    public FileIndex(final String indexFileAbsolutePath) throws IOException {
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

    private static NavigableMap<byte[], List<FileIndexEntry>> newMap() {
        return new TreeMap<byte[], List<FileIndexEntry>>(ByteUtils.COMPARATOR);
    }

    private static NavigableMap<byte[], List<FileIndexEntry>> fromBytes(final InputStream inputStream) throws IOException {
        final NavigableMap<byte[], List<FileIndexEntry>> ret = newMap();
        int nextByte;
        while ((nextByte=inputStream.read()) != -1) {
            final FileIndexEntry fileIndexEntry = FileIndexEntry.fromBytes(inputStream);
            addEntryToMap(ret, fileIndexEntry);
        }
        return ret;
    }

    public void addEntry(FileIndexEntry entry) throws IOException {
        // persist index entry to disk
        indexFileWriter.write(1);
        indexFileWriter.write(entry.toBytes());
        indexFileWriter.flush();

        // add to in-memory index
        addEntryToMap(index, entry);
    }

    private static void addEntryToMap(final NavigableMap<byte[], List<FileIndexEntry>> map, final FileIndexEntry entry) {
        List<FileIndexEntry> entries = map.get(entry.getStartKey());
        if (entries == null) {
            entries = new ArrayList<FileIndexEntry>(1);
        }
        entries.add(entry);
        map.put(entry.getStartKey(), entries);
    }

    /**
     * Return all files possibly containing any entries in the specified key ranges. This means all files whose start key
     * is less than exclEnd AND whose end key is greater than or equal to inclStart.
     */
    public List<FileIndexEntry> getFilesPossiblyContainingKeyRange(byte[] inclStart, byte[] exclEnd) {
        List<FileIndexEntry> ret = new ArrayList<FileIndexEntry>();
        final Map<byte[], List<FileIndexEntry>> headMap = exclEnd==null
                ? index.headMap(inclStart, true)
                : index.headMap(exclEnd, false);
        for (final Map.Entry<byte[], List<FileIndexEntry>> startKeyAndEntryList : headMap.entrySet()) {
            for (final FileIndexEntry fileIndexEntry : startKeyAndEntryList.getValue()) {
                if (exclEnd==null || ByteUtils.compare(fileIndexEntry.getEndKey(), inclStart) >= 0) {
                    ret.add(fileIndexEntry);
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
    public long getStartingIndexInFileForScan(byte[] inclStart, FileIndexEntry possibleHit) {
        return ByteUtils.compare(possibleHit.getStartKey(), inclStart) <= 0
                ? possibleHit.getKeyPositions().headMap(inclStart, true).lastEntry().getValue()
                : possibleHit.getKeyPositions().firstEntry().getValue();
    }

    public void close() throws IOException {
        indexFileWriter.close();
    }
}
