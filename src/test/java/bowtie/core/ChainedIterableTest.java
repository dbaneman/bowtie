package bowtie.core;

import bowtie.core.util.ChainedIterable;
import junit.framework.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 1/27/14
 * Time: 9:51 PM
 * To change this template use File | Settings | File Templates.
 */
public class ChainedIterableTest {

    @Test
    public void testIterator() throws Exception {
        List<Integer> list1 = Arrays.asList(1, 2, 3);
        List<Integer> list2 = Arrays.asList(4, 5);
        List<Integer> list3 = Arrays.asList(6);
        List<Integer> expected = Arrays.asList(1, 2, 3, 4, 5, 6);
        List<Integer> actual = new ArrayList<Integer>();
        ChainedIterable<Integer> chainedIterable = new ChainedIterable<Integer>(list1, list2, list3);
        for (Integer i : chainedIterable) {
            actual.add(i);
        }
        Assert.assertEquals(expected, actual);
    }

}
