package bowtie.core.util;

import bowtie.core.exceptions.MoreThanOneException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 1/27/14
 * Time: 9:45 PM
 * To change this template use File | Settings | File Templates.
 */
public class GetOne<T> {

    public T apply(Iterable<T> iterable) {
        final Iterator<T> iterator = iterable.iterator();
        final T next = iterator.next();
        if (iterator.hasNext()) {
            throw new MoreThanOneException(toCollection(iterable).size());
        }
        return next;
    }

    private Collection<T> toCollection(Iterable<T> iterable) {
        if (iterable instanceof Collection) {
            return (Collection<T>) iterable;
        }
        List<T> list = new ArrayList<T>();
        for (T t : iterable) {
            list.add(t);
        }
        return list;
    }
}
