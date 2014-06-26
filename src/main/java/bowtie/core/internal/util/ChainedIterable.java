package bowtie.core.internal.util;

import java.util.Arrays;
import java.util.Collections;
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
        return new ChainedIterator<T>(iterables);
    }

    public static class ChainedIterator<T> extends ReadAheadIterator<T> implements Iterator<T>  {
        private final List<Iterable<T>> iterables;
        int currentIteratorIndex;
        Iterator<T> currentIterator;

        ChainedIterator(final List<Iterable<T>> iterables) {
            this.iterables = iterables;
            currentIteratorIndex = -1;
            currentIterator = Collections.emptyIterator();
        }

        @Override
        protected T readAhead() throws Exception {
            if (currentIterator.hasNext()) {
                return currentIterator.next();
            }
            if (advanceToNextIterator()) {
                return readAhead();
            }
            return null;
        }

        boolean advanceToNextIterator() {
            if (++currentIteratorIndex < iterables.size()) {
                currentIterator = iterables.get(currentIteratorIndex).iterator();
                return true;
            }
            currentIterator = null;
            return false;
        }

    };
}
