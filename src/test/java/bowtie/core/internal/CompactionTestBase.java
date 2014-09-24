package bowtie.core.internal;

import bowtie.core.BowtieFactory;
import bowtie.core.Result;
import bowtie.core.Table;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by dan on 8/10/14.
 */
public abstract class CompactionTestBase {
    private static TableImpl table;
    private static List<Result> records;
    private static boolean didCompaction;

    protected abstract void doCompaction(Table table) throws IOException;

    @BeforeClass
    public static void setUp() throws Exception {
        Conf conf = new Conf();
        conf.set(Conf.BYTES_BETWEEN_INDEXED_KEYS, 15);
        conf.set(Conf.MAX_MEM_STORE_SIZE, 35);
        conf.set(Conf.MAX_DATA_FILE_SIZE, 85);
        table = (TableImpl) BowtieFactory.newTable(TableImplTest.TEST_TABLE_NAME, conf);
        if (table.exists()) {
            table.drop();
        }
        table.create();
        table.open();
        records = new ArrayList<Result>();
        records.add(ResultImpl.wrap(createByteArray(5, 1), createByteArray(8, 1))); // + 17 = 17
        records.add(ResultImpl.wrap(createByteArray(5, 2), createByteArray(3, 1))); // + 12  = 29
        records.add(ResultImpl.wrap(createByteArray(5, 3), createByteArray(9, 1))); // + 18 = 47
        records.add(ResultImpl.wrap(createByteArray(5, 4), createByteArray(40, 1))); // + 49 = 96
        records.add(ResultImpl.wrap(createByteArray(5, 5), createByteArray(15, 1))); // + 24 = 120
        records.add(ResultImpl.wrap(createByteArray(5, 6), createByteArray(11, 1))); // + 20 = 140
        records.add(ResultImpl.wrap(createByteArray(5, 7), createByteArray(27, 1))); // + 36 = 176
        records.add(ResultImpl.wrap(createByteArray(5, 8), createByteArray(2, 1))); // + 11 = 187
        records.add(ResultImpl.wrap(createByteArray(5, 9), createByteArray(15, 1))); // + 24 = 211
        for (int i=0; i<records.size(); i++) {
            put(i);
        }
        didCompaction = false;
    }

    @Before
    public void before() throws Exception {
        if (!didCompaction) {
            doCompaction(table);
            didCompaction = true;
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        table.close();
    }

    @Test
    public void testIndexEntriesAfterCompaction() throws Exception {
        Index index = table.fsTable.index;
        Assert.assertEquals(3, index.getAllEntries().size());
    }

    @Test
    public void testDataFilesAfterCompaction() throws Exception {
        int numDataFiles = new File(table.getConf().getDataDir(table.getName())).list().length;
        Assert.assertEquals(3, numDataFiles);
    }

    @Test
    public void testGetAfterCompaction() throws Exception {
        for (Result expected : records) {
            Assert.assertEquals(expected, table.get(expected.getKey()));
        }
    }

    @Test
    public void testScanAfterCompaction() throws Exception {
        Iterable<Result> resultScanner = table.scan(records.get(4).getKey(), records.get(6).getKey());
        Iterator<Result> iterator = resultScanner.iterator();
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals(records.get(4), iterator.next());
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals(records.get(5), iterator.next());
        Assert.assertFalse(iterator.hasNext());
    }

    private static byte[] createByteArray(int length, int byteToRepeat) {
        byte[] ret = new byte[length];
        for (int i=0; i<length; i++) {
            ret[i] = (byte) byteToRepeat;
        }
        return ret;
    }

    private static void put(int recordIndex) throws Exception {
        Result record = records.get(recordIndex);
        table.put(record.getKey(), record.getValue());
    }
}
