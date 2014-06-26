package bowtie.core.internal.util;

import org.apache.commons.collections.iterators.IteratorChain;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 1/27/14
 * Time: 9:22 PM
 * To change this template use File | Settings | File Templates.
 */
public class ChainedIterable<T> implements Iterable<T> {
    private final List<Iterable<T>> iterables;

    public ChainedIterable(List<Iterable<T>> iterables) {
        this.iterables = iterables;
    }

    public ChainedIterable(Iterable<T>... iterables) {
        this(Arrays.asList(iterables));
    }

    @Override
    public Iterator<T> iterator() {
        IteratorChain iteratorChain = new IteratorChain();
        for (Iterable<T> iterable : iterables) {
            iteratorChain.addIterator(iterable.iterator());
        }
        return iteratorChain;
    }

}
