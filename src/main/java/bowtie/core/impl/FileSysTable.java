package bowtie.core.impl;

import bowtie.core.api.external.IResult;
import bowtie.core.api.internal.IConf;
import bowtie.core.api.internal.IFileIndex;
import bowtie.core.api.internal.IFileReader;
import bowtie.core.api.internal.ITableReader;
import bowtie.core.util.ChainedIterable;
import bowtie.core.util.GetOne;

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
    private final IConf conf;
    private final IFileIndex fileIndex;
    private final IFileReader fileReader;
    private final GetOne<IResult> getOneResult;

    public FileSysTable(IConf conf, IFileIndex fileIndex, IFileReader fileReader) {
        this.conf = conf;
        this.fileIndex = fileIndex;
        this.fileReader = fileReader;
        getOneResult = new GetOne<IResult>();
    }

    @Override
    public IConf getConf() {
        return conf;
    }

    @Override
    public Iterable<IResult> scan(byte[] inclStart, byte[] exclStop) throws IOException {
        List<Iterable<IResult>> possibleHitIterators = new ArrayList<Iterable<IResult>>();
        for (String possibleHit : fileIndex.getFilesPossiblyContainingKeyRange(inclStart, exclStop)) {
            possibleHitIterators.add(fileReader.scanInFile(inclStart, exclStop, possibleHit));
        }
        return new ChainedIterable<IResult>(possibleHitIterators);
    }

    @Override
    public IResult get(byte[] key) throws IOException {
        Iterable<IResult> hits = scan(key, key);
        return getOneResult.get(hits);
    }

}
