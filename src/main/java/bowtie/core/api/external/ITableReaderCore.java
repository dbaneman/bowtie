package bowtie.core.api.external;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 2/10/14
 * Time: 11:01 PM
 * To change this template use File | Settings | File Templates.
 */
public interface ITableReaderCore extends IConfBacked {
    Iterable<IResult> scan(byte[] inclStart, byte[] exclStop) throws IOException;
}
