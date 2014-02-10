package bowtie.core;

import java.io.IOException;
import java.io.RandomAccessFile;
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
public class FileSysTable implements ITableReader {
    private final IConf conf;
    private final IFileIndex fileIndex;
    private final GetOne<IResult> getOneResult;

    public FileSysTable(IConf conf, IFileIndex fileIndex) {
        this.conf = conf;
        this.fileIndex = fileIndex;
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
            possibleHitIterators.add(scanInFile(inclStart, exclStop, possibleHit));
        }
        return new ChainedIterable<IResult>(possibleHitIterators);
    }

    private Iterable<IResult> scanInFile(byte[] inclStart, byte[] exclStop, String possibleHit) throws IOException {
        long position = fileIndex.getClosestPositionBeforeOrAtKeyRange(inclStart);
        RandomAccessFile file = new RandomAccessFile(getConf().getString(Conf.DATA_DIR) + possibleHit, "r");
        return new ScanIterable(file, position, exclStop);
    }

    @Override
    public IResult get(byte[] key) throws IOException {
        Iterable<IResult> hits = scan(key, key);
        return getOneResult.get(hits);
    }

    private static class ScanIterable implements Iterable<IResult> {
        private final RandomAccessFile file;
        private final byte[] exclStop;

        public ScanIterable(final RandomAccessFile file, final long startPosition, final byte[] exclStop) throws IOException {
            this.file = file;
            this.exclStop = exclStop;
            file.seek(startPosition);
        }

        @Override
        public Iterator<IResult> iterator() {
            return new ReadAheadIterator<IResult>() {

                @Override
                protected IResult readAhead() throws Exception {
                    short keyLength = file.readShort();
                    byte[] key = new byte[keyLength];
                    file.readFully(key);
                    short valueLength = file.readShort();
                    byte[] value = new byte[valueLength];
                    file.readFully(value);
                    if (ByteUtils.compare(key, exclStop) >= 0) {
                        return null;
                    }
                    return new Result(key, value);
                }

                @Override
                protected void onEnd() {
                    try {
                        file.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            };
        }
    }

}
