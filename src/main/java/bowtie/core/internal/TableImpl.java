package bowtie.core.internal;

import bowtie.core.Result;
import bowtie.core.Table;
import bowtie.core.TableReader;
import bowtie.core.exceptions.TableAlreadyExistsException;import bowtie.core.exceptions.TableDoesNotExistException;
import bowtie.core.internal.util.MergedNoDuplicatesIterable;
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
    private final String name;
    private final File tableDir;

    public TableImpl(final Conf conf, String name) {
        this.conf = conf;
        this.name = name;
        FileIndex fileIndex = new FileIndex();
        memTable = new MemTable(conf, fileIndex, name);
        fsTable = new FileSysTable(conf, fileIndex, new FileReader(conf, fileIndex, name));
        tableDir = new File(getConf().getDataDir(getName()));
    }

    public Conf getConf() {
        return conf;
    }

    @Override
    public boolean exists() {
        return tableDir.exists();
    }

    @Override
    public void create() throws IOException {
        if (exists()) {
            throw new TableAlreadyExistsException(getName());
        }
        FileUtils.forceMkdir(tableDir);
    }

    private void checkTableExists() {
        if (!exists()) {
            throw new TableDoesNotExistException(getName());
        }
    }

    @Override
    public void drop() throws IOException {
        checkTableExists();
        memTable.clear();
        FileUtils.deleteDirectory(tableDir);
    }

    @Override
    public void put(byte[] key, byte[] value) throws IOException {
        checkTableExists();
        memTable.put(key, value);
        if (memTable.isFull()) {
            memTable.flush();
        }
    }

    @Override
    public void delete(byte[] key) throws IOException {
        checkTableExists();
        memTable.delete(key);
    }

    @Override
    public void flush() throws IOException {
        checkTableExists();
        memTable.flush();
    }

    @Override
    public Iterable<Result> scan(final byte[] inclStart, final byte[] exclStop) throws IOException {
        checkTableExists();
        return new MergedNoDuplicatesIterable<Result>(ResultImpl.KEY_BASED_RESULT_COMPARATOR, memTable.scan(inclStart, exclStop), fsTable.scan(inclStart, exclStop));
    }

    @Override
    public Result get(byte[] key) throws IOException {
        checkTableExists();
        final ResultImpl memVal = (ResultImpl) memTable.get(key);
        return memVal.noVal() && !memVal.isDeleted()
                ? fsTable.get(key)
                : memVal;
    }

    @Override
    public String getName() {
        return name;
    }

}
