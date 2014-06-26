package bowtie.core.internal;

import bowtie.core.internal.util.ByteUtils;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 2/8/14
 * Time: 6:14 PM
 *
 * for every FileIndexEntry whose start key is <= target start key,
 *      if FIE end key is >= target end key
 *          return it
 */
public class FileIndex {
    private final NavigableMap<byte[], FileIndexEntry> index;

    public FileIndex() {
        this.index = new TreeMap<byte[], FileIndexEntry>(ByteUtils.getComparator());
    }

    public void addEntry(FileIndexEntry entry) {
        index.put(entry.getStartKey(), entry);
    }

    public Iterable<FileIndexEntry> getFilesPossiblyContainingKey(byte[] key) {
        return getFilesPossiblyContainingKeyRange(key, key);
    }

    public Iterable<FileIndexEntry> getFilesPossiblyContainingKeyRange(byte[] inclStart, byte[] exclEnd) {
        List<FileIndexEntry> ret = new ArrayList<FileIndexEntry>();
        for (Map.Entry<byte[], FileIndexEntry> kv : index.headMap(inclStart, true).entrySet()) {
             if (ByteUtils.compare(kv.getValue().getEndKey(), exclEnd) >= 0) {
                ret.add(kv.getValue());
             }
        }
        return ret;
    }

    public long getClosestPositionBeforeOrAtKey(byte[] inclStart, FileIndexEntry possibleHit) {
        return possibleHit.getKeyPositions().headMap(inclStart, true).lastEntry().getValue();
    }
}
