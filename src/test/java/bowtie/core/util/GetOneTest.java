package bowtie.core.util;

import bowtie.core.exceptions.MoreThanOneException;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.NoSuchElementException;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 3/5/14
 * Time: 9:54 PM
 * To change this template use File | Settings | File Templates.
 */
public class GetOneTest {
    private GetOne<Integer> getOne = new GetOne<Integer>();

    @Test (expected = NoSuchElementException.class)
    public void testApplyZero() throws Exception {
        getOne.apply(new ArrayList<Integer>());
    }

    @Test
    public void testApplyOne() throws Exception {
        Assert.assertEquals((Integer) 1, getOne.apply(Arrays.asList(1)));
    }

    @Test (expected = MoreThanOneException.class)
    public void testApplyMultiple() throws Exception {
        getOne.apply(Arrays.asList(1, 2));
    }
}
