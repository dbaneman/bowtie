package bowtie.core.util;

import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 2/8/14
 * Time: 9:56 PM
 * To change this template use File | Settings | File Templates.
 */
public abstract class ReadAheadIterator<T> implements Iterator<T> {
    private boolean isFirstIteration = true;
    private boolean alreadyReadAhead = false;
    private T next;

    protected void onStart() {
        // no op; subclasses may override
    }

    protected void onEnd() {
        // no op; subclasses may override
    }

    @Override
    public final boolean hasNext() {
        if (!alreadyReadAhead) {
            next = tryReadAhead();
            alreadyReadAhead = true;
        }
        return next!=null;
    }

    private T tryReadAhead() {
        try {
            if (isFirstIteration) {
                onStart();
                isFirstIteration = false;
            }
            T ret = readAhead();
            if (ret == null) {
                onEnd();
            }
            return ret;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public final T next() {
        if (!alreadyReadAhead) {
            next = tryReadAhead();
        }
        alreadyReadAhead = false;
        return next;
    }

    @Override
    public final void remove() {
        throw new UnsupportedOperationException();
    }

    protected abstract T readAhead() throws Exception;
}
