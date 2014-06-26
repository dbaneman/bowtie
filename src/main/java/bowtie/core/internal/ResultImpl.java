package bowtie.core.internal;

import bowtie.core.Result;

import java.util.Arrays;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 1/27/14
 * Time: 10:59 PM
 * To change this template use File | Settings | File Templates.
 */
public class ResultImpl implements Result {
    private final byte[] key;
    private final byte[] value;

    public ResultImpl(final byte[] key, final byte[] value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public byte[] getKey() {
        return key;
    }

    @Override
    public byte[] getValue() {
        return value;
    }

    @Override
    public boolean noVal() {
        return value == null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ResultImpl result = (ResultImpl) o;
        return Arrays.equals(key, result.key) && Arrays.equals(value, result.value);
    }

    @Override
    public int hashCode() {
        int result = key != null ? Arrays.hashCode(key) : 0;
        result = 31 * result + (value != null ? Arrays.hashCode(value) : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ResultImpl{" +
                "key=" + Arrays.toString(key) +
                ", value=" + Arrays.toString(value) +
                '}';
    }
}