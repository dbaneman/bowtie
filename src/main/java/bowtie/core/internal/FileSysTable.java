package bowtie.core.internal;

import bowtie.core.IResult;
import bowtie.core.ITableReader;
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
public class FileSysTable implements ITableReader {
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
    public Iterable<IResult> scan(byte[] inclStart, byte[] exclStop) throws IOException {
        List<Iterable<IResult>> possibleHitIterators = new ArrayList<Iterable<IResult>>();
        for (FileIndexEntry possibleHit : fileIndex.getFilesPossiblyContainingKeyRange(inclStart, exclStop)) {
            possibleHitIterators.add(fileReader.scanInFile(inclStart, exclStop, possibleHit));
        }
        return new ChainedIterable<IResult>(possibleHitIterators);
    }

    @Override
    public IResult get(byte[] key) throws IOException {
        IResult hit = null;
        for (FileIndexEntry possibleHit : fileIndex.getFilesPossiblyContainingKey(key)) {
            if (hit==null) {
                hit = fileReader.getInFile(key, possibleHit);
            } else {
                // TODO: implement this
                throw new RuntimeException("We don't yet support multiple values for the same key! Should pick latest based on timestamp or something.");
            }
        }
        return hit!=null ? hit : new Result(key, null);
    }

}
