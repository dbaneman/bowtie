package bowtie.core.internal;

import bowtie.core.Result;
import bowtie.core.TableReader;
import bowtie.core.internal.util.ChainedIterable;

import java.io.IOException;
import java.util.ArrayList;
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

    public FileSysTable(Conf conf, FileIndex fileIndex, FileReader fileReader) {
        this.conf = conf;
        this.fileIndex = fileIndex;
        this.fileReader = fileReader;
    }

    public Conf getConf() {
        return conf;
    }

    @Override
    public Iterable<Result> scan(byte[] inclStart, byte[] exclStop) throws IOException {
        List<Iterable<Result>> possibleHitIterables = new ArrayList<Iterable<Result>>();
        for (FileIndexEntry possibleHit : fileIndex.getFilesPossiblyContainingKeyRange(inclStart, exclStop)) {
            possibleHitIterables.add(fileReader.scanInFile(inclStart, exclStop, possibleHit));
        }
        return new ChainedIterable<Result>(possibleHitIterables);
    }

    @Override
    public Result get(byte[] key) throws IOException {
        Result hit = null;
        for (FileIndexEntry possibleHit : fileIndex.getFilesPossiblyContainingKey(key)) {
            if (hit==null) {
                hit = fileReader.getInFile(key, possibleHit);
            } else {
                // TODO: implement this
                throw new RuntimeException("We don't yet support multiple values for the same key! Should pick latest based on timestamp or something.");
            }
        }
        return hit!=null ? hit : new ResultImpl(key, null);
    }

}
