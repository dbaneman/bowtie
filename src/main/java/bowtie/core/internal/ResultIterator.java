package bowtie.core.internal;

import bowtie.core.Result;
import bowtie.core.internal.util.ByteUtils;
import bowtie.core.internal.util.ReadAheadIterator;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;

/**
* Created by dan on 8/8/14.
*/
public class ResultIterator extends ReadAheadIterator<Result> {
    protected final InputStream fileInputStream;
    private final long fileTimestamp;
    protected byte[] key;

    public ResultIterator(final String fileName, final long fileTimestamp) throws FileNotFoundException {
        this.fileInputStream = new BufferedInputStream(new FileInputStream(fileName));
        this.fileTimestamp = fileTimestamp;
    }

    @Override
    protected Result readAhead() throws Exception {
        byte[] value;
        byte[] length;
        length = new byte[ByteUtils.SIZE_DESCRIPTOR_LENGTH];
        fileInputStream.read(length);
        short keyLength = ByteBuffer.wrap(length).getShort();
        if (keyLength == ByteUtils.END_OF_FILE) {
            return null;
        }
        key = new byte[keyLength];
        fileInputStream.read(key);
        length = new byte[ByteUtils.SIZE_DESCRIPTOR_LENGTH];
        fileInputStream.read(length);
        short valueLength = ByteBuffer.wrap(length).getShort();
        if (valueLength == ByteUtils.DELETED_VALUE) {
            value = null;
        } else {
            value = new byte[valueLength];
            fileInputStream.read(value);
        }
        final boolean isDeleted = value == null;
        return new ResultImpl(key, value, fileTimestamp, isDeleted);
    }

    public static Iterable<Result> asIterable(final String fileName, final long fileTimestamp) {
        return new Iterable<Result>() {
            @Override
            public Iterator<Result> iterator() {
                try {
                    return new ResultIterator(fileName, fileTimestamp);
                } catch (FileNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }
}
