package bowtie.core.internal;

import bowtie.core.BowtieFactory;
import bowtie.core.Result;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 3/5/14
 * Time: 10:23 PM
 * To change this template use File | Settings | File Templates.
 */
public class TableImplTest {
    public static final String TEST_TABLE_NAME = "internal_test";
    TableImpl table;
    byte[] key = new byte[]{1, 2};
    byte[] key2 = new byte[]{2, 3, 4, 5, 7, 7, 8};
    byte[] value = new byte[]{10, 11, 12};
    byte[] value2 = new byte[]{20, 30, 40, 50};
    byte[] value3 = new byte[]{50, 60, 70, 80, 90};
    List<byte[]> orderedKeys = Arrays.asList(new byte[]{0}, new byte[]{1}, new byte[]{2}, new byte[]{3}, new byte[]{4});

    @Before
    public void before() throws Exception {
        table = (TableImpl) BowtieFactory.newTable(new Conf(), TEST_TABLE_NAME);
        if (table.exists()) {
            table.drop();
        }
        table.create();
        table.open();
    }

    @Test
    public void testPut() throws Exception {
        Assert.assertTrue(table.get(key).noVal());
        table.put(key, value);
        Assert.assertEquals(ResultImpl.wrap(key, value), table.get(key));
    }

    @Test
    public void testDelete() throws Exception {
        table.put(key, value);
        Assert.assertFalse(table.get(key).noVal());
        table.delete(key);
        Assert.assertTrue(table.get(key).noVal());
    }

    @Test
    public void testScan() throws Exception {
        Iterator<Result> results = table.scan(orderedKeys.get(1), orderedKeys.get(4)).iterator();
        Assert.assertFalse(results.hasNext());
        for (byte[] key : orderedKeys) {
            table.put(key, value);
        }
        results = table.scan(orderedKeys.get(1), orderedKeys.get(4)).iterator();
        Assert.assertTrue(results.hasNext());
        Assert.assertEquals(ResultImpl.wrap(orderedKeys.get(1), value), results.next());
        Assert.assertTrue(results.hasNext());
        Assert.assertEquals(ResultImpl.wrap(orderedKeys.get(2), value), results.next());
        Assert.assertTrue(results.hasNext());
        Assert.assertEquals(ResultImpl.wrap(orderedKeys.get(3), value), results.next());
        Assert.assertFalse(results.hasNext());
    }

    @Test
    public void testScanSingleValue() throws Exception {
        table.put(orderedKeys.get(2), value2);
        Iterator<Result> iterator = table.scan(orderedKeys.get(2), orderedKeys.get(3)).iterator();
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals(value2, iterator.next().getValue());
        Assert.assertFalse(iterator.hasNext());
    }

    @Test
    public void testScanSingleValueAfterFlush() throws Exception {
        table.put(orderedKeys.get(2), value);
        table.flush();
        Iterator<Result> iterator = table.scan(orderedKeys.get(2), orderedKeys.get(3)).iterator();
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals(ResultImpl.wrap(orderedKeys.get(2), value), iterator.next());
        Assert.assertFalse(iterator.hasNext());
    }

    @Test
    public void testGetSingle() throws Exception {
        Assert.assertTrue(table.get(key).noVal());
        table.put(key, value);
        Assert.assertEquals(ResultImpl.wrap(key, value), table.get(key));
    }

    @Test
    public void testGetAfterOverwrite() throws Exception {
        table.put(key, value);
        table.put(key, value2);
        Assert.assertEquals(ResultImpl.wrap(key, value2), table.get(key));
    }

    @Test
    public void testDrop() throws Exception {
        table.put(key, value);
        Assert.assertFalse(table.get(key).noVal());
        table.drop();
        table.create();
        Assert.assertTrue(table.get(key).noVal());
    }

    @Test
    public void testScanAfterFlush() throws Exception {
        for (byte[] key : orderedKeys) {
            table.put(key, value);
        }
        table.flush();
        Iterator<Result> results = table.scan(orderedKeys.get(1), orderedKeys.get(4)).iterator();
        Assert.assertTrue(results.hasNext());
        Assert.assertEquals(ResultImpl.wrap(orderedKeys.get(1), value), results.next());
        Assert.assertTrue(results.hasNext());
        Assert.assertEquals(ResultImpl.wrap(orderedKeys.get(2), value), results.next());
        Assert.assertTrue(results.hasNext());
        Assert.assertEquals(ResultImpl.wrap(orderedKeys.get(3), value), results.next());
        Assert.assertFalse(results.hasNext());
    }

    @Test
    public void testScanAfterSomeValuesFlushed() throws Exception {
        table.put(orderedKeys.get(0), value);
        table.put(orderedKeys.get(2), value);
        table.put(orderedKeys.get(4), value);
        table.flush();
        table.put(orderedKeys.get(1), value);
        table.put(orderedKeys.get(3), value);
        Iterator<Result> results = table.scan(orderedKeys.get(1), orderedKeys.get(4)).iterator();
        Assert.assertTrue(results.hasNext());
        Assert.assertEquals(ResultImpl.wrap(orderedKeys.get(1), value), results.next());
        Assert.assertTrue(results.hasNext());
        Assert.assertEquals(ResultImpl.wrap(orderedKeys.get(2), value), results.next());
        Assert.assertTrue(results.hasNext());
        Assert.assertEquals(ResultImpl.wrap(orderedKeys.get(3), value), results.next());
        Assert.assertFalse(results.hasNext());
    }

    @Test
    public void testScanAfterMultipleFlushes() throws Exception {
        table.put(orderedKeys.get(0), value);
        table.put(orderedKeys.get(2), value);
        table.flush();
        table.put(orderedKeys.get(1), value);
        table.put(orderedKeys.get(4), value);
        table.flush();
        table.put(orderedKeys.get(3), value);
        table.flush();
        Iterator<Result> results = table.scan(orderedKeys.get(1), orderedKeys.get(4)).iterator();
        Assert.assertTrue(results.hasNext());
        Assert.assertEquals(ResultImpl.wrap(orderedKeys.get(1), value), results.next());
        Assert.assertTrue(results.hasNext());
        Assert.assertEquals(ResultImpl.wrap(orderedKeys.get(2), value), results.next());
        Assert.assertTrue(results.hasNext());
        Assert.assertEquals(ResultImpl.wrap(orderedKeys.get(3), value), results.next());
        Assert.assertFalse(results.hasNext());
    }

    @Test
    public void testGetNullValue() throws Exception {
        Assert.assertTrue(table.get(key).noVal());
    }

    @Test
    public void testGetAfterFlush() throws Exception {
        table.put(key, value);
        table.flush();
        Assert.assertEquals(ResultImpl.wrap(key, value), table.get(key));
    }

    @Test
    public void testGetUpdateAfterOriginalValueFlushed() throws Exception {
        table.put(key, value);
        table.flush();
        table.put(key, value2);
        Assert.assertEquals(ResultImpl.wrap(key, value2), table.get(key));
    }

    @Test
    public void testGetFlushedUpdateAfterOriginalValueFlushed() throws Exception {
        table.put(key, value);
        table.flush();
        table.put(key, value2);
        table.flush();
        Assert.assertEquals(ResultImpl.wrap(key, value2), table.get(key));
    }

    @Test
    public void testGetMultipleUpdates() throws Exception {
        table.put(key, value);
        table.put(key, value2);
        table.put(key, value3);
        Assert.assertEquals(ResultImpl.wrap(key, value3), table.get(key));
    }

    @Test
    public void testGetMultipleFlushedUpdates() throws Exception {
        table.put(key, value);
        table.flush();
        table.put(key, value2);
        table.flush();
        table.put(key, value3);
        table.flush();
        Assert.assertEquals(ResultImpl.wrap(key, value3), table.get(key));
    }

    @Test
    public void testGetMultipleFlushedUpdatesWithSameStartKey() throws Exception {
        table.put(key, value);
        table.flush();
        table.put(key, value2);
        table.put(key2, value);
        table.flush();
        table.put(key, value3);
        table.flush();
        Assert.assertEquals(ResultImpl.wrap(key, value3), table.get(key));
        Assert.assertEquals(ResultImpl.wrap(key2, value), table.get(key2));
    }

    @Test
    public void testDeleteAfterFlush() throws Exception {
        table.put(key, value);
        table.delete(key);
        table.flush();
        Assert.assertTrue(table.get(key).noVal());
    }

    @Test
    public void testDeleteAfterOriginalValueFlushed() throws Exception {
        table.put(key, value);
        table.flush();
        table.delete(key);
        Assert.assertTrue(table.get(key).noVal());
    }

    @Test
    public void testFlushedDeleteAfterOriginalValueFlushed() throws Exception {
        table.put(key, value);
        table.flush();
        table.delete(key);
        table.flush();
        Assert.assertTrue(table.get(key).noVal());
    }

    @Test
    public void testGetAfterFlushingFalsePositives() throws Exception {
        table.put(orderedKeys.get(0), value);
        table.put(orderedKeys.get(4), value);
        table.flush();
        table.put(orderedKeys.get(2), value2);
        table.flush();
        Assert.assertEquals(ResultImpl.wrap(orderedKeys.get(2), value2), table.get(orderedKeys.get(2)));
    }

    @Test
    public void testScanAfterFlushingFalsePositives() throws Exception {
        table.put(orderedKeys.get(0), value);
        table.put(orderedKeys.get(4), value);
        table.flush();
        table.put(orderedKeys.get(2), value2);
        table.flush();
        Iterator<Result> iterator = table.scan(orderedKeys.get(2), orderedKeys.get(3)).iterator();
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals(ResultImpl.wrap(orderedKeys.get(2), value2), iterator.next());
        Assert.assertFalse(iterator.hasNext());
    }

    @Test
    public void testGetAfterShutdownAndRestart() throws Exception {
        table.put(key, value);
        restart();
        Assert.assertEquals(ResultImpl.wrap(key, value), table.get(key));
    }

    @Test
    public void testUpdateAfterShutdownAndRestart() throws Exception {
        table.put(key, value);
        table.put(key, value2);
        restart();
        Assert.assertEquals(ResultImpl.wrap(key, value2), table.get(key));
    }

    @Test
    public void testMultiFlushUpdateAfterShutdownAndRestart() throws Exception {
        table.put(key, value);
        table.flush();
        table.put(key, value2);
        restart();
        Assert.assertEquals(ResultImpl.wrap(key, value2), table.get(key));
    }

    @Test
    public void testMultiFlushUpdateWithMultipleEntriesAfterShutdownAndRestart() throws Exception {
        table.put(key, value);
        table.put(key2, value);
        table.flush();
        table.put(key, value2);
        table.put(key2, value2);
        restart();
        Assert.assertEquals(ResultImpl.wrap(key, value2), table.get(key));
        Assert.assertEquals(ResultImpl.wrap(key2, value2), table.get(key2));
    }

    @Test
    public void testDeleteAfterShutdownAndRestart() throws Exception {
        table.put(key, value);
        table.flush();
        table.delete(key);
        restart();
        Assert.assertTrue(table.get(key).noVal());
    }

    private void restart() throws Exception {
        table.close();
        table = new TableImpl(new Conf(), TEST_TABLE_NAME);
        table.open();
    }

}
