package bowtie.core.api.internal;

import bowtie.core.api.external.ITableReaderCore;
import bowtie.core.api.external.ITableWriterCore;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 2/10/14
 * Time: 11:06 PM
 * To change this template use File | Settings | File Templates.
 */
public interface IMemTable extends ITableReaderCore, ITableWriterCore {
    boolean isFull();

    // TODO: need a third map to handle reads while the file hasn't been flushed but
    void flush() throws IOException;
}
