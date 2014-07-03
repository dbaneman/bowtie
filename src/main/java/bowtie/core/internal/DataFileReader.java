package bowtie.core.internal;

import bowtie.core.Result;
import bowtie.core.internal.util.ByteUtils;
import bowtie.core.internal.util.ReadAheadIterator;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 2/10/14
 * Time: 10:39 PM
 * To change this template use File | Settings | File Templates.
 */
public class DataFileReader {
    private final Conf conf;
    private final Index index;
    private final String tableName;

    public DataFileReader(Conf conf, Index index, String tableName) {
        this.conf = conf;
        this.index = index;
        this.tableName = tableName;
    }

    public Iterable<Result> scanInFile(byte[] inclStart, byte[] exclStop, Index.Entry possibleHit) throws IOException {
        long position = index.getStartingIndexInFileForScan(inclStart, possibleHit);
        String fileLocation = getConf().getDataDir(tableName) + possibleHit.getFileName();
        return new ScanIterable(fileLocation, possibleHit.getFileTimestamp(), position, inclStart, exclStop);
    }

    public Conf getConf() {
        return conf;
    }

    private static class ScanIterable implements Iterable<Result> {
        private final InputStream fileInputStream;
        private final byte[] inclStart;
        private final byte[] exclStop;
        private final long fileTimestamp;

        public ScanIterable(final String fileLocation, final long fileTimestamp, final long startPosition, final byte[] inclStart, final byte[] exclStop) throws IOException {
            this.fileTimestamp = fileTimestamp;
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
                        length = new byte[ByteUtils.SIZE_DESCRIPTOR_LENGTH];
                        fileInputStream.read(length);
                        short keyLength = ByteBuffer.wrap(length).getShort();
                        if (keyLength == ByteUtils.END_OF_FILE) {
                            return null;
                        }
                        key = new byte[keyLength];
                        fileInputStream.read(key);
                        length = new byte[ByteUtils.SIZE_DESCRIPTOR_LENGTH];
                        fileInputStream.read(length);
                        short valueLength = ByteBuffer.wrap(length).getShort();
                        if (valueLength == ByteUtils.DELETED_VALUE) {
                            value = null;
                            continue;
                        }
                        value = new byte[valueLength];
                        fileInputStream.read(value);
                    } while (ByteUtils.compare(key, inclStart) < 0);
                    if ((exclStop==null && ByteUtils.compare(key, inclStart)!=0) || (exclStop!=null && ByteUtils.compare(key, exclStop) >= 0)) {
                        return null;
                    }
                    return new ResultImpl(key, value, fileTimestamp);
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
