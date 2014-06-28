package bowtie.core.internal;

import bowtie.core.Result;
import bowtie.core.TableReader;
import bowtie.core.internal.util.MergedNoDuplicatesIterable;

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
public class FileSysTable implements TableReader {
    private final Conf conf;
    private final FileIndex fileIndex;
    private final FileReader fileReader;

    public FileSysTable(final Conf conf, final FileIndex fileIndex, final FileReader fileReader) {
        this.conf = conf;
        this.fileIndex = fileIndex;
        this.fileReader = fileReader;
    }

    public Conf getConf() {
        return conf;
    }

    @Override
    public Iterable<Result> scan(final byte[] inclStart, final byte[] exclStop) throws IOException {
        final List<Iterable<Result>> possibleHitIterables = new ArrayList<Iterable<Result>>();
        for (final FileIndexEntry possibleHit : fileIndex.getFilesPossiblyContainingKeyRange(inclStart, exclStop)) {
            possibleHitIterables.add(fileReader.scanInFile(inclStart, exclStop, possibleHit));
        }
        return new MergedNoDuplicatesIterable<Result>(ResultImpl.KEY_BASED_RESULT_COMPARATOR, possibleHitIterables);
    }

    @Override
    public Result get(final byte[] key) throws IOException {
        final Iterator<Result> scanHits = scan(key, null).iterator();
        return scanHits.hasNext()
                ? scanHits.next()
                : new ResultImpl(key, null, ResultImpl.LATEST_FS_TIMESTAMP);
    }

}
