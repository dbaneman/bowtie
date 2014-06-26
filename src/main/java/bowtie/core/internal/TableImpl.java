package bowtie.core.internal;

import bowtie.core.Result;
import bowtie.core.Table;
import bowtie.core.TableReader;
import bowtie.core.internal.util.ChainedIterable;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 1/27/14
 * Time: 8:49 PM
 * To change this template use File | Settings | File Templates.
 */
public class TableImpl implements Table, TableReader {
    private final Conf conf;
    private final MemTable memTable;
    private final FileSysTable fsTable;

    public TableImpl(final Conf conf) {
        this.conf = conf;
        FileIndex fileIndex = new FileIndex();
        memTable = new MemTable(conf, fileIndex);
        fsTable = new FileSysTable(conf, fileIndex, new FileReader(conf, fileIndex));
    }

    public Conf getConf() {
        return conf;
    }

    @Override
    public void put(byte[] key, byte[] value) throws IOException {
        memTable.put(key, value);
        if (memTable.isFull()) {
            memTable.flush();
        }
    }

    @Override
    public void delete(byte[] key) throws IOException {
        memTable.delete(key);
    }

    @Override
    public void clear() throws IOException {
        memTable.clear();
        FileUtils.deleteDirectory(new File(getConf().getDataDir()));
    }

    @Override
    public Iterable<Result> scan(final byte[] inclStart, final byte[] exclStop) throws IOException {
        return new ChainedIterable<Result>(memTable.scan(inclStart, exclStop), fsTable.scan(inclStart, exclStop));
    }

    @Override
    public Result get(byte[] key) throws IOException {
        // TODO: this is only okay if we're certain that memVal is more recent
        final Result memVal = memTable.get(key);
        return memVal != null ? memVal : fsTable.get(key);
    }

}
