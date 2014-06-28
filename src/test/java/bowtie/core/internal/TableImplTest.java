package bowtie.core.internal;

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
    TableImpl table = new TableImpl(new Conf(), "foo");
    byte[] key = new byte[]{1};
    byte[] value = "foo".getBytes();
    byte[] value2 = "bar".getBytes();
    byte[] value3 = "bang".getBytes();
    List<byte[]> orderedKeys = Arrays.asList(new byte[]{0}, new byte[]{1}, new byte[]{2}, new byte[]{3}, new byte[]{4});

    @Before
    public void before() throws Exception {
        if (table.exists()) {
            table.drop();
        }
        table.create();
    }

    @Test
    public void testPut() throws Exception {
        Assert.assertTrue(table.get(key).noVal());
        table.put(key, value);
        Assert.assertEquals(new ResultImpl(key, value), table.get(key));
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
        Assert.assertEquals(new ResultImpl(orderedKeys.get(1), value), results.next());
        Assert.assertTrue(results.hasNext());
        Assert.assertEquals(new ResultImpl(orderedKeys.get(2), value), results.next());
        Assert.assertTrue(results.hasNext());
        Assert.assertEquals(new ResultImpl(orderedKeys.get(3), value), results.next());
        Assert.assertFalse(results.hasNext());
    }

    @Test
    public void testGetSingle() throws Exception {
        Assert.assertTrue(table.get(key).noVal());
        table.put(key, value);
        Assert.assertEquals(new ResultImpl(key, value), table.get(key));
    }

    @Test
    public void testGetAfterOverwrite() throws Exception {
        table.put(key, value);
        table.put(key, value2);
        Assert.assertEquals(new ResultImpl(key, value2), table.get(key));
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
        Assert.assertEquals(new ResultImpl(orderedKeys.get(1), value), results.next());
        Assert.assertTrue(results.hasNext());
        Assert.assertEquals(new ResultImpl(orderedKeys.get(2), value), results.next());
        Assert.assertTrue(results.hasNext());
        Assert.assertEquals(new ResultImpl(orderedKeys.get(3), value), results.next());
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
        Assert.assertEquals(new ResultImpl(orderedKeys.get(1), value), results.next());
        Assert.assertTrue(results.hasNext());
        Assert.assertEquals(new ResultImpl(orderedKeys.get(2), value), results.next());
        Assert.assertTrue(results.hasNext());
        Assert.assertEquals(new ResultImpl(orderedKeys.get(3), value), results.next());
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
        Assert.assertEquals(new ResultImpl(orderedKeys.get(1), value), results.next());
        Assert.assertTrue(results.hasNext());
        Assert.assertEquals(new ResultImpl(orderedKeys.get(2), value), results.next());
        Assert.assertTrue(results.hasNext());
        Assert.assertEquals(new ResultImpl(orderedKeys.get(3), value), results.next());
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
        Assert.assertEquals(new ResultImpl(key, value), table.get(key));
    }

    @Test
    public void testGetUpdateAfterOriginalValueFlushed() throws Exception {
        table.put(key, value);
        table.flush();
        table.put(key, value2);
        Assert.assertEquals(value2, table.get(key).getValue());
    }

    @Test
    public void testGetFlushedUpdateAfterOriginalValueFlushed() throws Exception {
        table.put(key, value);
        table.flush();
        table.put(key, value2);
        table.flush();
        Assert.assertEquals(value2, table.get(key).getValue());
    }

    @Test
    public void testGetMultipleUpdates() throws Exception {
        table.put(key, value);
        table.put(key, value2);
        table.put(key, value3);
        Assert.assertEquals(value3, table.get(key).getValue());
    }

    @Test
    public void testGetMultipleFlushedUpdates() throws Exception {
        table.put(key, value);
        table.flush();
        table.put(key, value2);
        table.flush();
        table.put(key, value3);
        table.flush();
        Assert.assertEquals(value3, table.get(key).getValue());
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

}
