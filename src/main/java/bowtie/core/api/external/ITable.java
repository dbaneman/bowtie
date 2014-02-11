package bowtie.core.api.external;

import bowtie.core.api.internal.ITableReader;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 1/27/14
 * Time: 8:45 PM
 * To change this template use File | Settings | File Templates.
 */
public interface ITable extends ITableReader {
    void put(byte[] key, byte[] value) throws IOException;
}
