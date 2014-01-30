package bowtie.core;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 1/27/14
 * Time: 8:49 PM
 * To change this template use File | Settings | File Templates.
 */
public class Table implements ITable, ITableReader {
    private final IConf conf;
    private final MemTable memTable;
    private final FileSysTable fsTable;

    public Table(final IConf conf) {
        this.conf = conf;
        memTable = new MemTable(conf);
        fsTable = new FileSysTable(conf);
    }

    @Override
    public IConf getConf() {
        return conf;
    }

    @Override
    public void put(final byte[] key, final byte[] value) {
        memTable.put(key, value);
        if (memTable.isFull()) {
            memTable.flush();
        }
    }

    @Override
    public Iterable<IResult> scan(final byte[] inclStart, final byte[] exclStop) {
        return new ChainedIterable<IResult>(memTable.scan(inclStart, exclStop), fsTable.scan(inclStart, exclStop));
    }

    @Override
    public IResult get(final byte[] key) {
        final IResult memVal = memTable.get(key);
        return memVal != null ? memVal : fsTable.get(key);
    }
}
