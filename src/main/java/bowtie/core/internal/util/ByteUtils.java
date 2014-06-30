package bowtie.core.internal.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Comparator;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 1/27/14
 * Time: 10:03 PM
 * To change this template use File | Settings | File Templates.
 */
public class ByteUtils {
    public static final short END_OF_FILE = -1;
    public static final short DELETED_VALUE = -2;
    public static final int SIZE_DESCRIPTOR_LENGTH = 2;

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

    public static Comparator<byte[]> COMPARATOR = new Comparator<byte[]>() {
        @Override
        public int compare(final byte[] o1, final byte[] o2) {
            return ByteUtils.compare(o1, o2);
        }
    };

    public static String writeBytes(InputStream bytes) throws IOException {
        StringBuilder sb = new StringBuilder();
        int next;
        while ((next=bytes.read()) != -1) {
            sb.append(next).append("\n");
        }
        return sb.toString();
    }

}
