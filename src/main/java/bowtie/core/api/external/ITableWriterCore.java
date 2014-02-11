package bowtie.core.api.external;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 2/10/14
 * Time: 11:01 PM
 * To change this template use File | Settings | File Templates.
 */
public interface ITableWriterCore extends IConfBacked {
    void append(byte[] key, byte[] value) throws IOException;
    void delete(byte[] key) throws IOException;
}
