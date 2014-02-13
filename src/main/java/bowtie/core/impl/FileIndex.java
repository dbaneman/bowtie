package bowtie.core.impl;

import bowtie.core.api.internal.IFileIndex;
import bowtie.core.api.internal.IFileIndexEntry;
import bowtie.core.util.ByteUtils;

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
public class FileIndex implements IFileIndex {
    private final NavigableMap<byte[], IFileIndexEntry> index;

    public FileIndex() {
        this.index = new TreeMap<byte[], IFileIndexEntry>(ByteUtils.getComparator());
    }

    @Override
    public void addEntry(IFileIndexEntry entry) {
        index.put(entry.getStartKey(), entry);
    }

    @Override
    public Iterable<IFileIndexEntry> getFilesPossiblyContainingKey(byte[] key) {
        return getFilesPossiblyContainingKeyRange(key, key);
    }

    @Override
    public Iterable<IFileIndexEntry> getFilesPossiblyContainingKeyRange(byte[] inclStart, byte[] exclEnd) {
        List<IFileIndexEntry> ret = new ArrayList<IFileIndexEntry>();
        for (Map.Entry<byte[], IFileIndexEntry> kv : index.headMap(inclStart, true).entrySet()) {
             if (ByteUtils.compare(kv.getValue().getEndKey(), exclEnd) >= 0) {
                ret.add(kv.getValue());
             }
        }
        return ret;
    }

    @Override
    public long getClosestPositionBeforeOrAtKey(byte[] inclStart, IFileIndexEntry possibleHit) {
        return possibleHit.getKeyPositions().headMap(inclStart, true).lastEntry().getValue();
    }
}
