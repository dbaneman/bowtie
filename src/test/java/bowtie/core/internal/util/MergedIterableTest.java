package bowtie.core.internal.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 1/27/14
 * Time: 9:51 PM
 * To change this template use File | Settings | File Templates.
 */
public class MergedIterableTest {
    private static final Comparator<Integer> INTEGER_COMPARATOR = new Comparator<Integer>() {
        @Override
        public int compare(Integer o1, Integer o2) {
            return o1.compareTo(o2);
        }
    };

    @Test
    public void testIterator() throws Exception {
        List<Integer> list1 = Arrays.asList(1, 2, 3);
        List<Integer> list2 = Arrays.asList(4, 5);
        List<Integer> list3 = Arrays.asList(6);
        List<Integer> expected = Arrays.asList(1, 2, 3, 4, 5, 6);
        List<Integer> actual = new ArrayList<Integer>();
        MergedIterable<Integer> chainedIterable = new MergedIterable<Integer>(INTEGER_COMPARATOR, list1, list2, list3);
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
        MergedIterable<Integer> chainedIterable = new MergedIterable<Integer>(INTEGER_COMPARATOR, list1, list2, list3, list4);
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
        Iterator<Integer> chainedIterableIterator  = new MergedIterable<Integer>(INTEGER_COMPARATOR, list1, list2, list3, list4).iterator();
        for (int i=0; i<6; i++) {
            actual.add(chainedIterableIterator.next());
        }
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testMultipleChainedIterables() throws Exception {
        List<Integer> iterable1 = Arrays.asList(1, 2);
        MergedIterable<Integer> iterable2 = new MergedIterable<Integer>(INTEGER_COMPARATOR);
        MergedIterable<Integer> iterable3 = new MergedIterable<Integer>(INTEGER_COMPARATOR, Arrays.asList(3,4), Arrays.asList(5,6));
        MergedIterable<Integer> chainedIterable = new MergedIterable<Integer>(INTEGER_COMPARATOR, iterable1, iterable2, iterable3);
        List<Integer> expected = Arrays.asList(1, 2, 3, 4, 5, 6);
        List<Integer> actual = new ArrayList<Integer>();
        for (Integer i : chainedIterable) {
            actual.add(i);
        }
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testDuplicates() throws Exception {
        List<Integer> iterable1 = Arrays.asList(1, 2, 3);
        List<Integer> iterable2 = Arrays.asList(3, 5, 7);
        List<Integer> iterable3 = Arrays.asList(2, 3, 5, 6, 8);
        List<Integer> iterable4 = Arrays.asList();
        List<Integer> iterable5 = Arrays.asList(8);
        MergedIterable<Integer> mergedIterable = new MergedIterable<Integer>(INTEGER_COMPARATOR, iterable1, iterable2, iterable3, iterable4, iterable5);
        List<Integer> expected = Arrays.asList(1, 2, 3, 5, 6, 7, 8);
        List<Integer> actual = new ArrayList<Integer>();
        for (Integer i : mergedIterable) {
            actual.add(i);
        }
        Assert.assertEquals(expected, actual);
    }

}
