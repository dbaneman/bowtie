package bowtie.core;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 1/27/14
 * Time: 9:14 PM
 * To change this template use File | Settings | File Templates.
 */
public interface ITableReader {
    Iterable<IResult> scan(byte[] inclStart, byte[] exclStop) throws IOException;
    IResult get(byte[] key) throws IOException;
}
