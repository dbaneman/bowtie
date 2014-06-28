package bowtie.core.internal;

import bowtie.core.Result;
import bowtie.core.internal.util.ByteUtils;

import java.util.Arrays;
import java.util.Comparator;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 1/27/14
 * Time: 10:59 PM
 * To change this template use File | Settings | File Templates.
 */
public class ResultImpl implements Result {
    public static final Long MEM_TIMESTAMP = Long.MAX_VALUE;
    public static final Long LATEST_FS_TIMESTAMP = MEM_TIMESTAMP - 1;
    public static Comparator<Result> KEY_BASED_RESULT_COMPARATOR = new Comparator<Result>() {
        @Override
        public int compare(Result o1, Result o2) {
            final int keyComparison = ByteUtils.compare(o1.getKey(), o2.getKey());
            return keyComparison != 0
                    ? keyComparison
                    : ((ResultImpl) o2).getTimestamp().compareTo(((ResultImpl) o1).getTimestamp());
        }
    };

    private final byte[] key;
    private final byte[] value;
    private final boolean isDeleted; // tells us if the value was found as a deleted value, as opposed to not being found at all (relevant because "not found" means we should keep checking older versions, but "deleted" means we're done looking)
    private final Long timestamp;

    public static ResultImpl wrap(final byte[] key, final byte[] value) {
        return new ResultImpl(key, value, null);
    }

    public ResultImpl(final byte[] key, final byte[] value, final Long timestamp) {
        this(key, value, timestamp, false);
    }

    public ResultImpl(final byte[] key, final byte[] value, final Long timestamp, final boolean isDeleted) {
        this.key = key;
        this.value = value;
        this.isDeleted = isDeleted;
        this.timestamp = timestamp;
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

    public boolean isDeleted() {
        return isDeleted;
    }

    public Long getTimestamp() {
        return timestamp;
    }
}
