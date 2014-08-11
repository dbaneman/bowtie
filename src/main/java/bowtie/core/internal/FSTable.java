package bowtie.core.internal;

import bowtie.core.Result;
import bowtie.core.TableReader;
import bowtie.core.internal.util.MergedIterable;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 1/27/14
 * Time: 9:02 PM
 * To change this template use File | Settings | File Templates.
 */
public class FSTable implements TableReader {
    private final Conf conf;
    private final String tableName;
    Index index;
    private final DataFileReader dataFileReader;

    public FSTable(final Conf conf, final String tableName, final Index index, final DataFileReader dataFileReader) {
        this.conf = conf;
        this.tableName = tableName;
        this.index = index;
        this.dataFileReader = dataFileReader;
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

    public void compactMinor() throws IOException {
        index.compactMinor();
        deletedUnindexedDataFiles();
    }

    public void compactMajor() throws IOException {
        final List<Index.Entry> compactedEntries = index.compact(index.getAllEntries());
        index.rewriteIndexFile(compactedEntries);
        index = Index.forFile(conf, tableName, index.getFilePath());
        deletedUnindexedDataFiles();
    }

    private void deletedUnindexedDataFiles() throws IOException {
        final Set<String> fileNames = new HashSet<String>();
        for (final Index.Entry indexEntry : index.getAllEntries()) {
            fileNames.add(indexEntry.getFileName());
        }
        for (final File dataFile : new File(conf.getDataDir(tableName)).listFiles()) {
            if (!fileNames.contains(dataFile.getName())) {
                FileUtils.forceDelete(dataFile);
            }
        }
    }
}
