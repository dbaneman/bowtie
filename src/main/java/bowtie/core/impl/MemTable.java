package bowtie.core.impl;

import bowtie.core.api.external.IConf;
import bowtie.core.api.external.IResult;
import bowtie.core.api.internal.IFileIndex;
import bowtie.core.api.internal.IFileIndexEntry;
import bowtie.core.api.internal.IMemTable;
import bowtie.core.util.ByteUtils;
import bowtie.core.util.ChainedIterable;
import bowtie.core.util.ReadAheadIterator;
import com.eaio.uuid.UUID;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 1/27/14
 * Time: 8:48 PM
 * To change this template use File | Settings | File Templates.
 */
public class MemTable implements IMemTable {
    private SortedMap<byte[], Collection<byte[]>> map;
    private SortedMap<byte[], Collection<byte[]>> mapCurrentlyFlushing;
    private final IConf conf;
    private final IFileIndex fileIndex;
    private long size;

    public MemTable(final IConf conf, final IFileIndex fileIndex) {
        this.conf = conf;
        this.fileIndex = fileIndex;
        map = newMap();
        size = 0;
    }

    private static SortedMap<byte[], Collection<byte[]>> newMap() {
        return new TreeMap<byte[], Collection<byte[]>>(ByteUtils.getComparator());
    }

    @Override
    public IConf getConf() {
        return conf;
    }

    @Override
    public Iterable<IResult> scan(byte[] inclStart, byte[] exclStop) {
        return mapCurrentlyFlushing!=null
                ? new ChainedIterable<IResult>(scan(inclStart, exclStop, map), scan(inclStart, exclStop, mapCurrentlyFlushing))
                : scan(inclStart, exclStop, map);
    }

    private static Iterable<IResult> scan(byte[] inclStart, byte[] exclStop, SortedMap<byte[], Collection<byte[]>> map) {
        final Iterator<Map.Entry<byte[], Collection<byte[]>>> subMapIterator = exclStop!=null
                ? map.subMap(inclStart, exclStop).entrySet().iterator()
                : Collections.singletonList((Map.Entry<byte[], Collection<byte[]>>) new AbstractMap.SimpleEntry<byte[], Collection<byte[]>>(inclStart, map.get(inclStart))).iterator();
        return new Iterable<IResult>() {
            @Override
            public Iterator<IResult> iterator() {
                return new ReadAheadIterator<IResult>() {
                    private byte[] currentKey;
                    private Iterator<byte[]> iteratorWithinCollection = Collections.emptyListIterator();

                    @Override
                    protected IResult readAhead() throws Exception {
                        while (!iteratorWithinCollection.hasNext()) {
                            if (!subMapIterator.hasNext()) {
                                return null;
                            }
                            final Map.Entry<byte[], Collection<byte[]>> next = subMapIterator.next();
                            currentKey = next.getKey();
                            iteratorWithinCollection = next.getValue().iterator();
                        }
                        return new Result(currentKey, iteratorWithinCollection.next());
                    }
                };
            }
        };
    }

    @Override
    public void append(byte[] key, byte[] value) {
        Collection<byte[]> existingValue = map.get(key);
        // since we expect most keys to just have one value, we want the singleton list to be optimized.
        // TODO: optimize it! singletonList() may not be the best solution.
        if (existingValue == null) {
            existingValue = Collections.singletonList(value);
        } else if (existingValue.size() == 1) {
            existingValue = new ArrayList<byte[]>(existingValue);
        }
        existingValue.add(value);
        size += key.length + value.length;
    }

    @Override
    public void delete(byte[] key) throws IOException {
        throw new UnsupportedOperationException();
        // TODO: append tombstone record
    }

    @Override
    public boolean isFull() {
        return size >= getConf().getLong(Conf.MAX_MEM_STORE_SIZE);
    }

    @Override
    public synchronized void flush() throws IOException {
        // from this point on, writes go to a new map. until this is done flushing, reads will come from both the new and flushing map.
        mapCurrentlyFlushing = map;
        map = newMap();

        // flush map to file
        IFileIndexEntry fileIndexEntry = new FileIndexEntry();
        String fileName = new UUID().toString();
        fileIndexEntry.setFileName(fileName);
        OutputStream fileOutputStream = new FileOutputStream(getConf().getString(Conf.DATA_DIR) + "/" + fileName);
        boolean first = true;
        long currentSize = 0;
        for (Map.Entry<byte[], Collection<byte[]>> entry : mapCurrentlyFlushing.entrySet()) {
            for (byte[] val : entry.getValue()) {
                if (first) {
                    fileIndexEntry.setStartKey(entry.getKey());
                    first = false;
                }
                int sizeOfEntry = 2 + entry.getKey().length + 2 + val.length;
                ByteBuffer byteBuffer = ByteBuffer.allocate(sizeOfEntry);
                byteBuffer.putShort((short) entry.getKey().length);
                byteBuffer.put(entry.getKey());
                byteBuffer.putShort((short) val.length);
                byteBuffer.put(val);
                fileOutputStream.write(byteBuffer.array());
                fileIndexEntry.setEndKey(entry.getKey());
                if (currentSize > getConf().getLong(Conf.BYTES_BETWEEN_INDEXED_KEYS)) {
                    fileIndexEntry.addIndexedKey(entry.getKey(), currentSize);
                    currentSize = 0;
                }
                currentSize += sizeOfEntry;
            }
        }
        fileOutputStream.close();

        // update file index
        fileIndex.addEntry(fileIndexEntry);
    }
}
