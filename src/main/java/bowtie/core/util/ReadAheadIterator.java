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
    private boolean isFirstIteration = false;
    private boolean alreadyReadAhead = false;
    private T next;

    protected void onStart() {
        // no op; subclasses may override
    }

    protected void onEnd() {
        // no op; subclasses may override
    }

    @Override
    public boolean hasNext() {
        if (isFirstIteration) {
            onStart();
            isFirstIteration = false;
        }
        if (!alreadyReadAhead) {
            next = tryReadAhead();
            alreadyReadAhead = true;
        }
        boolean ret = next!=null;
        if (!ret) {
            onEnd();
        }
        return ret;
    }

    private T tryReadAhead() {
        try {
            return readAhead();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public T next() {
        if (!alreadyReadAhead) {
            next = tryReadAhead();
        }
        alreadyReadAhead = false;
        return next;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    protected abstract T readAhead() throws Exception;
}
