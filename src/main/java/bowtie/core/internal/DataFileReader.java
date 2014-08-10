package bowtie.core.internal;

import bowtie.core.Result;
import bowtie.core.internal.util.ByteUtils;

import java.io.*;
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

    public Iterable<Result> scanInFile(final byte[] inclStart, final byte[] exclStop, final Index.Entry possibleHit) throws IOException {
        final long position = index.getStartingIndexInFileForScan(inclStart, possibleHit);
        final String fileLocation = conf.getDataDir(tableName) + possibleHit.getFileName();
        return new Iterable<Result>() {
            @Override
            public Iterator<Result> iterator() {
                try {
                    return new ScanIterator(fileLocation, possibleHit.getFileTimestamp(), position, inclStart, exclStop);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    private static class ScanIterator extends ResultIterator {
        private final byte[] inclStart;
        private final byte[] exclStop;

        public ScanIterator(final String fileLocation, final long fileTimestamp, final long startPosition, final byte[] inclStart, final byte[] exclStop) throws IOException {
            super(fileLocation, fileTimestamp);
            this.inclStart = inclStart;
            this.exclStop = exclStop;
            fileInputStream.skip(startPosition);
        }


        @Override
        protected Result readAhead() throws Exception {
            Result result;
            do {
                result = super.readAhead();
            } while (result!=null && !((ResultImpl) result).isDeleted() && ByteUtils.compare(key, inclStart) < 0);
            if (result == null) {
                return null;
            }
            if ((exclStop==null && ByteUtils.compare(key, inclStart)!=0) || (exclStop!=null && ByteUtils.compare(key, exclStop) >= 0)) {
                return null;
            }
            return result;
        }

        @Override
        protected void onEnd() {
            try {
                fileInputStream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
