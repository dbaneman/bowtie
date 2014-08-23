package bowtie.core.exceptions;

import bowtie.core.internal.util.ByteUtils;

/**
 * Created by dan on 8/23/14.
 */
public class InvalidScanParametersException extends Exception {

    public InvalidScanParametersException(byte[] suppliedStart, byte[] suppliedEnd) {
        super("Scan start is lexicographically equal or greater than end. Start=[ " + ByteUtils.writeBytes(suppliedStart, " ") + "], end=[" + ByteUtils.writeBytes(suppliedEnd, " ") + "]");
    }

}
