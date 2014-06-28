package bowtie.core.internal;

import bowtie.core.internal.util.ByteUtils;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 2/8/14
 * Time: 6:14 PM
 */
public class FileIndex {
    private final NavigableMap<byte[], FileIndexEntry> index;

    public FileIndex() {
        this.index = new TreeMap<byte[], FileIndexEntry>(ByteUtils.COMPARATOR);
    }

    public void addEntry(FileIndexEntry entry) {
        index.put(entry.getStartKey(), entry);
    }

    /**
     * Return all files possibly containing any entries in the specified key ranges. This means all files whose start key
     * LT exclEnd AND whose end key GE inclStart. These files are returned as a priority queue, with priority given to the file
     * with the latest timestamp.
     */
    public List<FileIndexEntry> getFilesPossiblyContainingKeyRange(byte[] inclStart, byte[] exclEnd) {
        List<FileIndexEntry> ret = new ArrayList<FileIndexEntry>();
        final Map<byte[], FileIndexEntry> headMap = exclEnd==null
                ? index.headMap(inclStart, true)
                : index.headMap(exclEnd, false);
        for (Map.Entry<byte[], FileIndexEntry> kv : headMap.entrySet()) {
            if (exclEnd==null || ByteUtils.compare(kv.getValue().getEndKey(), inclStart) >= 0) {
                ret.add(kv.getValue());
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
}
