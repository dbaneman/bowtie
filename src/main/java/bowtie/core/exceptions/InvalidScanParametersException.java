package bowtie.core.exceptions;

import bowtie.core.internal.util.ByteUtils;

/**
 * Thrown if users executes a scan where the start key isn't lexicographically prior to the end key.
 */
public class InvalidScanParametersException extends Exception {

    public InvalidScanParametersException(byte[] suppliedStart, byte[] suppliedEnd) {
        super("Scan start is lexicographically equal or greater than end. Start=[ " + ByteUtils.writeBytes(suppliedStart, " ") + "], end=[" + ByteUtils.writeBytes(suppliedEnd, " ") + "]");
    }

}
