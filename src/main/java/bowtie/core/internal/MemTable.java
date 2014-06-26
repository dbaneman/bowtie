package bowtie.core.internal;

import bowtie.core.Result;
import bowtie.core.Table;
import bowtie.core.internal.util.ByteUtils;
import bowtie.core.internal.util.ChainedIterable;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 1/27/14
 * Time: 8:48 PM
 * To change this template use File | Settings | File Templates.
 */
public class MemTable implements Table {
    private NavigableMap<byte[], byte[]> map;
    private NavigableMap<byte[], byte[]> mapCurrentlyFlushing;
    private final Conf conf;
    private final FileIndex fileIndex;
    private long size;

    public MemTable(final Conf conf, final FileIndex fileIndex) {
        this.conf = conf;
        this.fileIndex = fileIndex;
        map = newMap();
        size = 0;
    }

    private static NavigableMap<byte[], byte[]> newMap() {
        return new TreeMap<byte[], byte[]>(ByteUtils.getComparator());
    }

    public Conf getConf() {
        return conf;
    }

    @Override
    public Iterable<Result> scan(byte[] inclStart, byte[] exclStop) {
        return mapCurrentlyFlushing!=null
                ? new ChainedIterable<Result>(scan(inclStart, exclStop, map), scan(inclStart, exclStop, mapCurrentlyFlushing))
                : scan(inclStart, exclStop, map);
    }

    @Override
    public Result get(byte[] key) throws IOException {
        if (key == null) {
            throw new NullPointerException("Attempted to get a null key.");
        }
        return new ResultImpl(key, map.get(key));
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
                        return new ResultImpl(next.getKey(), next.getValue());
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
        byte[] value = map.get(key);
        map.remove(key);
        size -= (key.length + value.length);
    }

    @Override
    public void clear() throws IOException {
        map.clear();
        if (mapCurrentlyFlushing != null) {
            mapCurrentlyFlushing.clear();
        }
    }

    public boolean isFull() {
        return size >= getConf().getLong(Conf.MAX_MEM_STORE_SIZE);
    }

    public synchronized void flush() throws IOException {
        if (map.isEmpty()) {
            return;
        }

        // from this point on, writes go to a new map. until this is done flushing, reads will come from both the new and flushing map.
        mapCurrentlyFlushing = map;
        map = newMap();

        // flush map to file
        FileIndexEntry fileIndexEntry = createFileIndexEntry();
        OutputStream fileOutputStream = new BufferedOutputStream(new FileOutputStream(getConf().getDataDir() + "/" + fileIndexEntry.getFileName()));
        long currentSize = 0;
        byte[] currentEntryBytes;
        fileIndexEntry.setStartKey(mapCurrentlyFlushing.firstKey());
        fileIndexEntry.setEndKey(mapCurrentlyFlushing.lastKey());
        for (Map.Entry<byte[], byte[]> entry : mapCurrentlyFlushing.entrySet()) {
            currentEntryBytes = encodeEntry(entry);
            fileOutputStream.write(currentEntryBytes);
            if (currentSize > getConf().getLong(Conf.BYTES_BETWEEN_INDEXED_KEYS)) {
                fileIndexEntry.addIndexedKey(entry.getKey(), currentSize);
                currentSize = 0;
            }
            currentSize += currentEntryBytes.length;
        }
        fileOutputStream.close();

        // update file index
        fileIndex.addEntry(fileIndexEntry);
    }

    private FileIndexEntry createFileIndexEntry() {
        FileIndexEntry fileIndexEntry = new FileIndexEntry();
        Long timestampForFile;
        String fileName;
        File file;
        do {
            timestampForFile = System.currentTimeMillis();
            fileName = getConf().getDataDir() + "/" + timestampForFile;
            file = new File(fileName);
        } while (!file.exists());
        fileIndexEntry.setFileName(timestampForFile.toString());
        return fileIndexEntry;
    }

    private byte[] encodeEntry(Map.Entry<byte[], byte[]> entry) {
        int sizeOfEntry = 2 + entry.getKey().length + 2 + entry.getValue().length;
        ByteBuffer byteBuffer = ByteBuffer.allocate(sizeOfEntry);
        byteBuffer.putShort((short) entry.getKey().length);
        byteBuffer.put(entry.getKey());
        byteBuffer.putShort((short) entry.getValue().length);
        byteBuffer.put(entry.getValue());
        return byteBuffer.array();
    }
}
