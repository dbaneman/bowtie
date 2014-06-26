package bowtie.core.internal.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 3/5/14
 * Time: 10:01 PM
 * To change this template use File | Settings | File Templates.
 */
public class ByteUtilsTest {
    byte[] b1 = new byte[]{1,2,3};
    byte[] b2 = new byte[]{1,2};
    byte[] b3 = new byte[]{2};
    byte[] b4 = new byte[]{1,2};

    @Test
    public void testCompare() throws Exception {

        Assert.assertEquals(0, ByteUtils.compare(b2, b4));
        Assert.assertEquals(0, ByteUtils.compare(b4, b2));
        Assert.assertTrue(ByteUtils.compare(b1, b2) > 0);
        Assert.assertTrue(ByteUtils.compare(b2, b1) < 0);
        Assert.assertTrue(ByteUtils.compare(b1, b3) < 0);
        Assert.assertTrue(ByteUtils.compare(b3, b1) > 0);
        Assert.assertTrue(ByteUtils.compare(b2, b3) < 0);
        Assert.assertTrue(ByteUtils.compare(b3, b2) > 0);
    }

    @Test
    public void testGetComparator() throws Exception {
        SortedMap<byte[], Integer> map = new TreeMap<byte[], Integer>(ByteUtils.COMPARATOR);
        map.put(b1, 0);
        map.put(b2, 0);
        map.put(b3, 0);
        map.put(b4, 0);
        Iterator<Map.Entry<byte[], Integer>> entrySet = map.entrySet().iterator();
        Assert.assertTrue(Arrays.equals(b2, entrySet.next().getKey()));
        Assert.assertTrue(entrySet.next().getKey() == b1);
        Assert.assertTrue(entrySet.next().getKey() == b3);
    }
}
