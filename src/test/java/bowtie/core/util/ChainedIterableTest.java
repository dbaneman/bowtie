package bowtie.core.util;

import junit.framework.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
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

    @Test
    public void testIteratorWithEmpty() throws Exception {
        List<Integer> list1 = Arrays.asList(1, 2, 3);
        List<Integer> list2 = Arrays.asList();
        List<Integer> list3 = Arrays.asList(4, 5);
        List<Integer> list4 = Arrays.asList(6);
        List<Integer> expected = Arrays.asList(1, 2, 3, 4, 5, 6);
        List<Integer> actual = new ArrayList<Integer>();
        ChainedIterable<Integer> chainedIterable = new ChainedIterable<Integer>(list1, list2, list3, list4);
        for (Integer i : chainedIterable) {
            actual.add(i);
        }
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testIteratorWithoutHasNext() throws Exception {
        List<Integer> list1 = Arrays.asList(1, 2, 3);
        List<Integer> list2 = Arrays.asList();
        List<Integer> list3 = Arrays.asList(4, 5);
        List<Integer> list4 = Arrays.asList(6);
        List<Integer> expected = Arrays.asList(1, 2, 3, 4, 5, 6);
        List<Integer> actual = new ArrayList<Integer>();
        Iterator<Integer> chainedIterableIterator  = new ChainedIterable<Integer>(list1, list2, list3, list4).iterator();
        for (int i=0; i<6; i++) {
            actual.add(chainedIterableIterator.next());
        }
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testMultipleChainedIterables() throws Exception {
        List<Integer> iterable1 = Arrays.asList(1, 2);
        ChainedIterable<Integer> iterable2 = new ChainedIterable<Integer>();
        ChainedIterable<Integer> iterable3 = new ChainedIterable<Integer>(Arrays.asList(3,4), Arrays.asList(5,6));
        ChainedIterable<Integer> chainedIterable = new ChainedIterable<Integer>(iterable1, iterable2, iterable3);
        List<Integer> expected = Arrays.asList(1, 2, 3, 4, 5, 6);
        List<Integer> actual = new ArrayList<Integer>();
        for (Integer i : chainedIterable) {
            actual.add(i);
        }
        Assert.assertEquals(expected, actual);
    }

}
