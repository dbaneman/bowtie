package bowtie.core;

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
        return new ChainedIterator<T>(iterables);
    }

    public static class ChainedIterator<T> implements Iterator<T>  {
        private final List<Iterable<T>> iterables;
        int currentIteratorIndex;
        Iterator<T> currentIterator;

        ChainedIterator(final List<Iterable<T>> iterables) {
            this.iterables = iterables;
            currentIteratorIndex = -1;
            advanceToNextIterator();
        }

        @Override
        public boolean hasNext() {
            return currentIterator.hasNext() || advanceToNextIterator() && currentIterator.hasNext();
        }

        @Override
        public T next() {
            return currentIterator.next();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
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
