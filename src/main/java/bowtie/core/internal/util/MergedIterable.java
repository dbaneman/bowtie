package bowtie.core.internal.util;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 1/27/14
 * Time: 9:22 PM
 * Wrapper for a collection of sorted iterables. This class's iterator's next() method will return the next value
 * (according to the supplied comparator) across all iterables. No duplicates are allowed, so if multiple iterables
 * have the same value (according to the supplied comparator), only the first will be returned.
 */
public class MergedIterable<T> implements Iterable<T> {
    private final PriorityQueue<PeekingIterator<T>> iteratorHeap;
    private final Comparator<T> elementComparator;
    private final boolean allowDuplicates;

    public MergedIterable(Comparator<T> comparator, List<Iterable<T>> iterables) {
        this(comparator, iterables, false);
    }
    public MergedIterable(Comparator<T> comparator, List<Iterable<T>> iterables, boolean allowDuplicates) {
        this.allowDuplicates = allowDuplicates;
        this.elementComparator = comparator;
        int iteratorHeapSize = iterables.isEmpty()
                ? 1
                : iterables.size();
        this.iteratorHeap = new PriorityQueue<PeekingIterator<T>>(iteratorHeapSize, createIteratorComparator());
        for (final Iterable<T> iterable : iterables) {
            final Iterator<T> iterator = iterable.iterator();
            if (iterator.hasNext()) {
                iteratorHeap.offer(new PeekingIterator<T>(iterator));
            }
        }
    }

    private Comparator<PeekingIterator<T>> createIteratorComparator() {
        return new Comparator<PeekingIterator<T>>() {
            @Override
            public int compare(PeekingIterator<T> iterator1, PeekingIterator<T> iterator2) {
                if (!iterator1.hasNext()) {
                    return iterator2.hasNext()
                            ? -1
                            : 0;
                }
                if (!iterator2.hasNext()) {
                    return iterator1.hasNext()
                            ? -1
                            : 0;
                }
                return elementComparator.compare(iterator1.peek(), iterator2.peek());
            }
        };
    }

    public MergedIterable(Comparator<T> comparator, Iterable<T>... iterables) {
        this(comparator, Arrays.asList(iterables));
    }

    @Override
    public Iterator<T> iterator() {
        return new Iterator<T>() {
            @Override
            public boolean hasNext() {
                return !iteratorHeap.isEmpty();
            }

            @Override
            public T next() {
                final PeekingIterator<T> nextIteratorInHeap = iteratorHeap.poll();
                final T nextElement = advanceIteratorInHeap(nextIteratorInHeap);
                if (!allowDuplicates) {
                    while (!iteratorHeap.isEmpty() && elementComparator.compare(iteratorHeap.peek().peek(), nextElement) == 0) {
                        advanceIteratorInHeap(iteratorHeap.poll());
                    }
                }
                return nextElement;
            }

            private T advanceIteratorInHeap(PeekingIterator<T> iterator) {
                final T nextElement = iterator.next();
                if (iterator.hasNext()) {
                    iteratorHeap.offer(iterator);
                }
                return nextElement;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

}
