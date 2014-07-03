package bowtie.core;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 1/27/14
 * Time: 8:45 PM
 * To change this template use File | Settings | File Templates.
 */
public interface Table extends TableReader, TableWriter {
    void create() throws IOException;
    void drop() throws IOException;
    String getName();
    boolean exists();

    void open() throws IOException;

    void beginCompaction(CompactionType compactionType, boolean ignoreSilentlyIfAlreadyCompacting) throws IOException;

    void waitUntilCompactionComplete() throws InterruptedException;

    void waitUntilCompactionComplete(long timeout) throws InterruptedException;
}
