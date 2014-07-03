package bowtie.core.internal;

import bowtie.core.CompactionType;
import bowtie.core.Result;
import bowtie.core.TableReader;
import bowtie.core.internal.util.MergedIterable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 1/27/14
 * Time: 9:02 PM
 * To change this template use File | Settings | File Templates.
 */
public class FSTable implements TableReader {
    private final Conf conf;
    private final Index index;
    private final DataFileReader dataFileReader;

    public FSTable(final Conf conf, final Index index, final DataFileReader dataFileReader) {
        this.conf = conf;
        this.index = index;
        this.dataFileReader = dataFileReader;
    }

    public Conf getConf() {
        return conf;
    }

    @Override
    public Iterable<Result> scan(final byte[] inclStart, final byte[] exclStop) throws IOException {
        final List<Iterable<Result>> possibleHitIterables = new ArrayList<Iterable<Result>>();
        for (final Index.Entry possibleHit : index.getFilesPossiblyContainingKeyRange(inclStart, exclStop)) {
            possibleHitIterables.add(dataFileReader.scanInFile(inclStart, exclStop, possibleHit));
        }
        return new MergedIterable<Result>(ResultImpl.KEY_BASED_RESULT_COMPARATOR, possibleHitIterables);
    }

    @Override
    public Result get(final byte[] key) throws IOException {
        final Iterator<Result> scanHits = scan(key, null).iterator();
        return scanHits.hasNext()
                ? scanHits.next()
                : new ResultImpl(key, null, ResultImpl.LATEST_FS_TIMESTAMP);
    }

    @Override
    public void close() throws IOException {
        index.close();
    }

    public void beginCompaction(CompactionType compactionType, boolean ignoreSilentlyIfAlreadyCompacting) throws IOException {
        switch (compactionType) {
            case MAJOR :
                beginMajorCompaction();
                break;
            case MINOR :
                beginMinorCompaction();
                break;
            default :
                break;
        }

        // TODO: make this run in a separate thread; then the wait/notify stuff will become relevant
        notifyAll();
    }

    private void beginMajorCompaction() {

    }

    private void beginMinorCompaction() {

    }

    public void waitUntilCompactionComplete() throws InterruptedException {
        wait();
    }

    public void waitUntilCompactionComplete(long timeout) throws InterruptedException {
        wait(timeout);
    }

}
