package bowtie.core;

import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 1/27/14
 * Time: 9:22 PM
 * To change this template use File | Settings | File Templates.
 */
public class ChainedIterable<T> implements Iterable<T> {
    private final Iterable<T>[] iterables;

    public ChainedIterable(Iterable<T>... iterables) {
        this.iterables = iterables;
    }

    @Override
    public Iterator<T> iterator() {
        return new ChainedIterator<T>(iterables);
    }

    public static class ChainedIterator<T> implements Iterator<T>  {
        private final Iterable<T>[] iterables;
        int currentIteratorIndex;
        Iterator<T> currentIterator;

        ChainedIterator(final Iterable<T>[] iterables) {
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
            if (++currentIteratorIndex < iterables.length) {
                currentIterator = iterables[currentIteratorIndex].iterator();
                return true;
            }
            currentIterator = null;
            return false;
        }

    };
}
