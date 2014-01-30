package bowtie.core;

import java.util.Comparator;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 1/27/14
 * Time: 10:03 PM
 * To change this template use File | Settings | File Templates.
 */
public class ByteUtils {

    public static int compare(final byte[] left, final byte[] right) {
        for (int i = 0, j = 0; i < left.length && j < right.length; i++, j++) {
            int a = (left[i] & 0xff);
            int b = (right[j] & 0xff);
            if (a != b) {
                return a - b;
            }
        }
        return left.length - right.length;
    }

    private static Comparator<byte[]> comparator;
    public static Comparator<byte[]> getComparator() {
        if (comparator==null) {
            comparator = new Comparator<byte[]>() {
                @Override
                public int compare(byte[] o1, byte[] o2) {
                    return ByteUtils.compare(o1, o2);
                }
            };
        }
        return comparator;
    }

}
