package bowtie.core.impl;

import bowtie.core.api.external.IResult;
import org.junit.Assert;
import org.junit.After;
import org.junit.Test;

import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 3/5/14
 * Time: 10:23 PM
 * To change this template use File | Settings | File Templates.
 */
public class TableTest {
    Table table = new Table(new Conf());
    byte[] key = new byte[]{1};
    byte[] value = new byte[]{2};
    byte[] value2 = new byte[]{10};

    @Test
    public void testPut() throws Exception {
        Assert.assertTrue(table.get(key).noVal());
        table.put(key, value);
        Assert.assertEquals(new Result(key, value), table.get(key));
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
        byte[] key0 = new byte[]{0};
        byte[] key1 = new byte[]{1};
        byte[] key2 = new byte[]{2};
        byte[] key3 = new byte[]{3};
        byte[] key4 = new byte[]{4};
        Iterator<IResult> results = table.scan(key1, key4).iterator();
        Assert.assertFalse(results.hasNext());
        table.put(key0, value);
        table.put(key1, value);
        table.put(key2, value);
        table.put(key3, value);
        table.put(key4, value);
        results = table.scan(key1, key4).iterator();
        Assert.assertTrue(results.hasNext());
        Assert.assertEquals(new Result(key1, value), results.next());
        Assert.assertTrue(results.hasNext());
        Assert.assertEquals(new Result(key2, value), results.next());
        Assert.assertTrue(results.hasNext());
        Assert.assertEquals(new Result(key3, value), results.next());
        Assert.assertFalse(results.hasNext());
    }

    @Test
    public void testGetSingle() throws Exception {
        Assert.assertTrue(table.get(key).noVal());
        table.put(key, value);
        Assert.assertEquals(new Result(key, value), table.get(key));
    }

    @Test
    public void testGetAfterOverwrite() throws Exception {
        table.put(key, value);
        table.put(key, value2);
        Assert.assertEquals(new Result(key, value2), table.get(key));
    }

    @Test
    public void testClear() throws Exception {
        table.put(key, value);
        Assert.assertFalse(table.get(key).noVal());
        table.clear();
        Assert.assertTrue(table.get(key).noVal());
    }

    @After
    public void after() throws Exception {
        table.clear();
    }
}
