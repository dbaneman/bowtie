package bowtie.core.internal;

import bowtie.core.Result;
import bowtie.core.TableReader;
import bowtie.core.TableWriter;
import bowtie.core.internal.util.ByteUtils;
import bowtie.core.internal.util.DataWriterUtil;

import java.io.*;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 1/27/14
 * Time: 8:48 PM
 * To change this template use File | Settings | File Templates.
 */
public class MemTable implements TableReader, TableWriter {
    private NavigableMap<byte[], byte[]> map;
    private final Conf conf;
    private final Index index;
    private long size;
    private String name;

    public MemTable(final Conf conf, final Index index, final String name) {
        this.conf = conf;
        this.name = name;
        this.index = index;
        map = newMap();
        size = 0;
    }

    private static NavigableMap<byte[], byte[]> newMap() {
        return new TreeMap<byte[], byte[]>(ByteUtils.COMPARATOR);
    }

    @Override
    public Iterable<Result> scan(byte[] inclStart, byte[] exclStop) {
        return scan(inclStart, exclStop, map);
    }

    @Override
    public Result get(byte[] key) throws IOException {
        if (key == null) {
            throw new NullPointerException("Attempted to get a null key.");
        }
        final byte[] value = map.get(key);
        // since we designate deleted keys by putting a null value, we can distinguish between an absent value and a deleted value by checking if the map contains the specified key
        final boolean isDeleted = value==null && map.containsKey(key);
        return new ResultImpl(key, value, ResultImpl.MEM_TIMESTAMP, isDeleted);
    }

    @Override
    public void close() throws IOException {
        flush();
    }

    private static Iterable<Result> scan(byte[] inclStart, byte[] exclStop, SortedMap<byte[], byte[]> map) {
        final Iterator<Map.Entry<byte[], byte[]>> subMapIterator = map.subMap(inclStart, exclStop).entrySet().iterator();
        return new Iterable<Result>() {
            @Override
            public Iterator<Result> iterator() {
                return new Iterator<Result>() {
                    @Override
                    public boolean hasNext() {
                        return subMapIterator.hasNext();
                    }

                    @Override
                    public Result next() {
                        final Map.Entry<byte[], byte[]> next = subMapIterator.next();
                        return new ResultImpl(next.getKey(), next.getValue(), ResultImpl.MEM_TIMESTAMP);
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    @Override
    public void put(byte[] key, byte[] value) {
        if (key == null) {
            throw new NullPointerException("Attempted to put a null key.");
        }
        if (value == null) {
            throw new NullPointerException("Attempted to put a null value.");
        }
        map.put(key, value);
        size += key.length + value.length;
    }

    @Override
    public void delete(byte[] key) throws IOException {
        if (key == null) {
            throw new NullPointerException("Attempted to delete a null key.");
        }
        // we designate deleted keys by putting a null value
        map.put(key, null);
        size += key.length;
    }

    public void clear() throws IOException {
        map.clear();
    }

    public boolean isFull() {
        return size >= conf.getLong(Conf.MAX_MEM_STORE_SIZE);
    }

    @Override
    public void flush() throws IOException {
        if (map.isEmpty()) {
            return;
        }

        // flush map to file
        final List<Index.Entry> fileIndexEntries = DataWriterUtil.writeDataFilesAndCreateIndexEntries(conf, name, map.entrySet());

        // update file index
        for (final Index.Entry fileIndexEntry : fileIndexEntries) {
            index.addEntry(fileIndexEntry);
        }

        // clear memtable
        map.clear();
    }

}
