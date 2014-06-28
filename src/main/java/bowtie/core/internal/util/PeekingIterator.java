package bowtie.core.internal.util;

import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 6/28/14
 * Time: 1:51 PM
 * To change this template use File | Settings | File Templates.
 */
public class PeekingIterator<T> implements Iterator<T> {
    private final Iterator<T> wrappedIterator;
    private T nextElement;

    public PeekingIterator(final Iterator<T> wrappedIterator) {
        this.wrappedIterator = wrappedIterator;
        readNextElement();
    }

    private void readNextElement() {
        nextElement = wrappedIterator.hasNext()
                ? wrappedIterator.next()
                : null;
    }

    @Override
    public boolean hasNext() {
        return nextElement != null;
    }

    @Override
    public T next() {
        final T ret = nextElement;
        readNextElement();
        return ret;
    }

    public T peek() {
        return nextElement;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
