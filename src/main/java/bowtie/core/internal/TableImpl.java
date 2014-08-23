package bowtie.core.internal;

import bowtie.core.Result;
import bowtie.core.Table;
import bowtie.core.exceptions.*;
import bowtie.core.internal.util.ByteUtils;
import bowtie.core.internal.util.MergedIterable;
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
public class TableImpl implements Table {
    private static final String INDEX_FILE_LOCAL_NAME = "index";
    private final Conf conf;
    private MemTable memTable;
    FSTable fsTable;
    private final String name;
    private File tableDir;
    private boolean open = false;

    public TableImpl(final Conf conf, String name) throws IOException {
        this.conf = conf;
        this.name = name;
        tableDir = new File(getConf().getDataDir(getName()));
    }

    @Override
    public void open() throws IOException {
        if (open) {
            throw new TableAlreadyOpenException(getName());
        }
        final Index index = Index.forFile(conf, name, getConf().getDataDir(getName()) + INDEX_FILE_LOCAL_NAME);
        memTable = new MemTable(conf, index, name);
        fsTable = new FSTable(conf, name, index, new DataFileReader(conf, index, name));
        open = true;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public void compactMinor() throws IOException {
        fsTable.compactMinor();
    }

    @Override
    public void compactMajor() throws IOException {
        fsTable.compactMajor();
    }

    public Conf getConf() {
        return conf;
    }

    @Override
    public boolean exists() {
        return tableDir.exists();
    }

    @Override
    public void close() throws IOException {
        checkTableExistsAndOpen();
        memTable.close();
        fsTable.close();
        open = false;
    }

    @Override
    public void create() throws IOException {
        if (exists()) {
            throw new TableAlreadyExistsException(getName());
        }
        FileUtils.forceMkdir(tableDir);
    }

    private void checkTableExistsAndOpen() {
        checkOpen();
        checkTableExists();
    }

    private void checkOpen() {
        if (!open) {
            throw new ClosedTableException(getName());
        }
    }

    @Override
    public void drop() throws IOException {
        checkTableExists();
        if (memTable != null) {
            memTable.clear();
        }
        FileUtils.deleteDirectory(tableDir);
    }

    private void checkTableExists() {
        if (!exists()) {
            throw new TableDoesNotExistException(getName());
        }
    }

    @Override
    public void put(byte[] key, byte[] value) throws IOException {
        checkTableExistsAndOpen();
        memTable.put(key, value);
        if (memTable.isFull()) {
            memTable.flush();
        }
    }

    @Override
    public void delete(byte[] key) throws IOException {
        checkTableExistsAndOpen();
        memTable.delete(key);
    }

    @Override
    public void flush() throws IOException {
        checkTableExistsAndOpen();
        memTable.flush();
    }

    @Override
    public Iterable<Result> scan(final byte[] inclStart, final byte[] exclStop) throws IOException, InvalidScanParametersException {
        checkTableExistsAndOpen();
        checkValidScan(inclStart, exclStop);
        return new MergedIterable<Result>(ResultImpl.KEY_BASED_RESULT_COMPARATOR, memTable.scan(inclStart, exclStop), fsTable.scan(inclStart, exclStop));
    }

    private static void checkValidScan(final byte[] inclStart, final byte[] exclStop) throws InvalidScanParametersException {
        if (ByteUtils.compare(inclStart, exclStop) >= 0) {
            throw new InvalidScanParametersException(inclStart, exclStop);
        }
    }

    @Override
    public Result get(byte[] key) throws IOException {
        checkTableExistsAndOpen();
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
