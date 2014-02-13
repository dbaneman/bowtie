package bowtie.core.impl;

import bowtie.core.api.external.IResult;
import bowtie.core.api.external.IConf;
import bowtie.core.api.internal.IFileIndex;
import bowtie.core.api.internal.IFileReader;
import bowtie.core.util.ByteUtils;
import bowtie.core.util.GetOne;
import bowtie.core.util.ReadAheadIterator;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 2/10/14
 * Time: 10:39 PM
 * To change this template use File | Settings | File Templates.
 */
public class FileReader implements IFileReader {
    private final IConf conf;
    private final IFileIndex fileIndex;
    private final GetOne<IResult> getOneResult;

    public FileReader(IConf conf, IFileIndex fileIndex) {
        this.conf = conf;
        this.fileIndex = fileIndex;
        getOneResult = new GetOne<IResult>();
    }

    @Override
    public Iterable<IResult> scanInFile(byte[] inclStart, byte[] exclStop, String possibleHit) throws IOException {
        long position = fileIndex.getClosestPositionBeforeOrAtKey(inclStart);
        RandomAccessFile file = new RandomAccessFile(getConf().getString(Conf.DATA_DIR) + possibleHit, "r");
        return new ScanIterable(file, position, inclStart, exclStop);
    }

    @Override
    public IResult getInFile(byte[] key, String possibleHit) throws IOException {
        return getOneResult.apply(scanInFile(key, null, possibleHit));
    }

    @Override
    public IConf getConf() {
        return conf;
    }

    private static class ScanIterable implements Iterable<IResult> {
        private final RandomAccessFile file;
        private final byte[] inclStart;
        private final byte[] exclStop;

        public ScanIterable(final RandomAccessFile file, final long startPosition, final byte[] inclStart, final byte[] exclStop) throws IOException {
            this.file = file;
            this.inclStart = inclStart;
            this.exclStop = exclStop;
            file.seek(startPosition);
        }

        @Override
        public Iterator<IResult> iterator() {
            return new ReadAheadIterator<IResult>() {

                @Override
                protected IResult readAhead() throws Exception {
                    byte[] key;
                    byte[] value;
                    do {
                        short keyLength = file.readShort();
                        key = new byte[keyLength];
                        file.readFully(key);
                        short valueLength = file.readShort();
                        value = new byte[valueLength];
                        file.readFully(value);
                    } while (ByteUtils.compare(key, inclStart) < 0);
                    if ((exclStop==null && ByteUtils.compare(key, inclStart)!=0) || ByteUtils.compare(key, exclStop) >= 0) {
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
