package bowtie.core.impl;

import bowtie.core.api.external.IResult;
import bowtie.core.api.external.ITable;
import bowtie.core.api.external.IConf;
import bowtie.core.api.internal.IFileIndex;
import bowtie.core.api.external.ITableReader;
import bowtie.core.util.ChainedIterable;
import bowtie.core.util.GetOne;

import java.io.IOException;

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
    private final GetOne<IResult> getOneResult;

    public Table(final IConf conf) {
        this.conf = conf;
        IFileIndex fileIndex = new FileIndex();
        memTable = new MemTable(conf, fileIndex);
        fsTable = new FileSysTable(conf, fileIndex, new FileReader(conf, fileIndex));
        getOneResult = new GetOne<IResult>();
    }

    @Override
    public IConf getConf() {
        return conf;
    }

    @Override
    public void append(byte[] key, byte[] value) throws IOException {
        memTable.append(key, value);
        if (memTable.isFull()) {
            memTable.flush();
        }
    }

    @Override
    public void put(final byte[] key, final byte[] value) throws IOException {
        delete(key);
        append(key, value);
    }

    @Override
    public void delete(byte[] key) throws IOException {
        memTable.delete(key);
    }

    @Override
    public Iterable<IResult> scan(final byte[] inclStart, final byte[] exclStop) throws IOException {
        return new ChainedIterable<IResult>(memTable.scan(inclStart, exclStop), fsTable.scan(inclStart, exclStop));
    }

    @Override
    public Iterable<IResult>get(final byte[] key) throws IOException {
        return scan(key, null);
    }

    @Override
    public IResult getOne(byte[] key) throws IOException {
        final Iterable<IResult> hits = get(key);
        return getOneResult.apply(hits);

    }

    @Override
    public IResult scanOne(byte[] inclStart, byte[] exclStop) throws IOException {
        final Iterable<IResult> hits = scan(inclStart, exclStop);
        return getOneResult.apply(hits);
    }

}
