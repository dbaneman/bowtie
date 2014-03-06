package bowtie.core.util;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 3/5/14
 * Time: 9:41 PM
 * To change this template use File | Settings | File Templates.
 */
public class ReadAheadIteratorTest {
    ReadAheadIterator<String> readAheadIterator;
    Iterator<Integer> integers;
    String onStart;
    String onEnd;

    @Before
    public void before() throws Exception {
        integers = Arrays.asList(1, 2, 3, 4, 5).iterator();
        readAheadIterator = new ReadAheadIterator<String>() {
            @Override
            protected String readAhead() throws Exception {
                return integers.hasNext() ? integers.next().toString() : null;
            }
            @Override
            protected void onStart() {
                onStart = "foo";
            }
            @Override
            protected void onEnd() {
                onEnd = "bar";
            }
        };
    }

    @Test
    public void testOnStart1() throws Exception {
        Assert.assertNull(onStart);
        readAheadIterator.hasNext();
        Assert.assertEquals("foo", onStart);
    }

    @Test
    public void testOnStart2() throws Exception {
        Assert.assertNull(onStart);
        readAheadIterator.next();
        Assert.assertEquals("foo", onStart);
    }

    @Test
    public void testOnEnd() throws Exception {
        while (readAheadIterator.hasNext()) {
            Assert.assertNull(onEnd);
            readAheadIterator.next();
        }
        Assert.assertEquals("bar", onEnd);
    }

    @Test
    public void testHasNext() throws Exception {
        for (int i=0; i<5; i++) {
            Assert.assertTrue(readAheadIterator.hasNext());
            readAheadIterator.next();
        }
        Assert.assertFalse(readAheadIterator.hasNext());
    }

    @Test
    public void testNext() throws Exception {
        for (Integer i=1; i<=5; i++) {
            Assert.assertEquals(i.toString(), readAheadIterator.next());
        }
    }

    @Test (expected = UnsupportedOperationException.class)
    public void testRemove() throws Exception {
        readAheadIterator.remove();
    }

}
