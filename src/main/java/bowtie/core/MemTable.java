package bowtie.core;

import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 1/27/14
 * Time: 8:48 PM
 * To change this template use File | Settings | File Templates.
 */
public class MemTable implements ITable {
    private SortedMap<byte[], byte[]> map;
    private final IConf conf;
    private long size;

    public MemTable(final IConf conf) {
        this.conf = conf;
        resetMap();
        size = 0;
    }

    private void resetMap() {
        map = new TreeMap<byte[], byte[]>(ByteUtils.getComparator());
    }

    @Override
    public IConf getConf() {
        return conf;
    }

    @Override
    public Iterable<IResult> scan(byte[] inclStart, byte[] exclStop) {
        final Iterator<Map.Entry<byte[], byte[]>> subMapIterator = map.subMap(inclStart, exclStop).entrySet().iterator();
        return new Iterable<IResult>() {
            @Override
            public Iterator<IResult> iterator() {
                return new Iterator<IResult>() {
                    @Override
                    public boolean hasNext() {
                        return subMapIterator.hasNext();
                    }

                    @Override
                    public IResult next() {
                        final Map.Entry<byte[], byte[]> next = subMapIterator.next();
                        return new Result(next.getKey(), next.getValue());
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
        map.put(key, value);
        size += key.length + value.length;
    }

    @Override
    public IResult get(byte[] key) {
        return new Result(key, map.get(key));
    }

    public boolean isFull() {
        return size >= getConf().getLong(Conf.MAX_MEM_STORE_SIZE);
    }

    public void flush() {
        SortedMap<byte[], byte[]> mapForFlushing = map;
        resetMap();
        for (Map.Entry<byte[], byte[]> entry : mapForFlushing.entrySet()) {

        }
    }
}
