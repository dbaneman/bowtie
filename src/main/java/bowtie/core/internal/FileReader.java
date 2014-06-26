package bowtie.core.internal;

import bowtie.core.Result;
import bowtie.core.internal.util.ByteUtils;
import bowtie.core.internal.util.GetOne;
import bowtie.core.internal.util.ReadAheadIterator;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 2/10/14
 * Time: 10:39 PM
 * To change this template use File | Settings | File Templates.
 */
public class FileReader {
    private final Conf conf;
    private final FileIndex fileIndex;
    private final GetOne<Result> getOneResult;
    private final String tableName;

    public FileReader(Conf conf, FileIndex fileIndex, String tableName) {
        this.conf = conf;
        this.fileIndex = fileIndex;
        getOneResult = new GetOne<Result>();
        this.tableName = tableName;
    }

    public Iterable<Result> scanInFile(byte[] inclStart, byte[] exclStop, FileIndexEntry possibleHit) throws IOException {
        long position = fileIndex.getClosestPositionBeforeOrAtKey(inclStart, possibleHit);
        String fileLocation = getConf().getDataDir(tableName) + possibleHit.getFileName();
        return new ScanIterable(fileLocation, position, inclStart, exclStop);
    }

    public Result getInFile(byte[] key, FileIndexEntry possibleHit) throws IOException {
        return getOneResult.apply(scanInFile(key, null, possibleHit));
    }

    public Conf getConf() {
        return conf;
    }

    private static class ScanIterable implements Iterable<Result> {
        private final InputStream fileInputStream;
        private final byte[] inclStart;
        private final byte[] exclStop;

        public ScanIterable(final String fileLocation, final long startPosition, final byte[] inclStart, final byte[] exclStop) throws IOException {
            this.fileInputStream = new BufferedInputStream(new FileInputStream(fileLocation));
            this.inclStart = inclStart;
            this.exclStop = exclStop;
            fileInputStream.skip(startPosition);
        }

        @Override
        public Iterator<Result> iterator() {
            return new ReadAheadIterator<Result>() {

                @Override
                protected Result readAhead() throws Exception {
                    byte[] key;
                    byte[] value;
                    byte[] length;
                    do {
                        length = new byte[2];
                        fileInputStream.read(length);
                        short keyLength = ByteBuffer.wrap(length).getShort();
                        if (keyLength == -1) {
                            return null;
                        }
                        key = new byte[keyLength];
                        fileInputStream.read(key);
                        length = new byte[2];
                        fileInputStream.read(length);
                        short valueLength = ByteBuffer.wrap(length).getShort();
                        value = new byte[valueLength];
                        fileInputStream.read(value);
                    } while (ByteUtils.compare(key, inclStart) < 0);
                    if ((exclStop==null && ByteUtils.compare(key, inclStart)!=0) || (exclStop!=null && ByteUtils.compare(key, exclStop) >= 0)) {
                        return null;
                    }
                    return new ResultImpl(key, value);
                }

                @Override
                protected void onEnd() {
                    try {
                        fileInputStream.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            };
        }
    }

}
