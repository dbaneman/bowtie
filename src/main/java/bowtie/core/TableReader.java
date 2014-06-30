package bowtie.core;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 1/27/14
 * Time: 9:14 PM
 * To change this template use File | Settings | File Templates.
 */
public interface TableReader {
    Iterable<Result> scan(byte[] inclStart, byte[] exclStop) throws IOException;
    Result get(byte[] key) throws IOException;
    void close() throws IOException;
}
