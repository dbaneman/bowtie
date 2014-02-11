package bowtie.core.impl;

import bowtie.core.api.external.IResult;
import bowtie.core.api.external.IConf;
import bowtie.core.api.internal.IFileIndex;
import bowtie.core.api.internal.IFileReader;
import bowtie.core.util.ByteUtils;
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

    public FileReader(IConf conf, IFileIndex fileIndex) {
        this.conf = conf;
        this.fileIndex = fileIndex;
    }

    @Override
    public Iterable<IResult> scanInFile(byte[] inclStart, byte[] exclStop, String possibleHit) throws IOException {
        long position = fileIndex.getClosestPositionBeforeOrAtKeyRange(inclStart);
        RandomAccessFile file = new RandomAccessFile(getConf().getString(Conf.DATA_DIR) + possibleHit, "r");
        return new ScanIterable(file, position, exclStop);
    }

    @Override
    public IConf getConf() {
        return conf;
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
