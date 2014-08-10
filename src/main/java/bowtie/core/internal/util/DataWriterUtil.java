package bowtie.core.internal.util;

import bowtie.core.internal.Conf;
import bowtie.core.internal.Index;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by dan on 8/10/14.
 */
public class DataWriterUtil {
    public static List<Index.Entry> writeDataFilesAndCreateIndexEntries(final Conf conf, final String tableName, final Iterable<? extends Map.Entry<byte[], byte[]>> records) throws IOException {
        final List<Index.Entry> outputEntries = new ArrayList<Index.Entry>();
        final Iterator<? extends Map.Entry<byte[], byte[]>> recordIterator = records.iterator();
        while (recordIterator.hasNext()) {
            final Index.Entry fileIndexEntry = writeDataFileAndCreateIndexEntry(conf, tableName, recordIterator);
            outputEntries.add(fileIndexEntry);
        }
        return outputEntries;
    }

    private static Index.Entry writeDataFileAndCreateIndexEntry(final Conf conf, final String tableName, final Iterator<? extends Map.Entry<byte[], byte[]>> recordIterator) throws IOException {
        final long bytesBetweenIndexedKeys = conf.getLong(Conf.BYTES_BETWEEN_INDEXED_KEYS);
        long maxFileSize = conf.getLong(Conf.MAX_DATA_FILE_SIZE);
        Index.Entry fileIndexEntry = createFileIndexEntry(conf, tableName);
        OutputStream fileOutputStream = new BufferedOutputStream(new FileOutputStream(conf.getDataDir(tableName) + fileIndexEntry.getFileName()));
        long bytesSinceLastPosIndex = 0;
        long currentSizeTotal = 0;
        byte[] currentEntryBytes;
        boolean first = true;
        Map.Entry<byte[], byte[]> lastRecord = null;
        Map.Entry<byte[], byte[]> record;
        while (recordIterator.hasNext()) {
            record = recordIterator.next();
            if (first) {
                fileIndexEntry.setStartKey(record.getKey());
                first = false;
            }
            currentEntryBytes = encodeEntry(record);
            fileOutputStream.write(currentEntryBytes);
            if (bytesSinceLastPosIndex > bytesBetweenIndexedKeys) {
                fileIndexEntry.addIndexedKey(record.getKey(), bytesSinceLastPosIndex);
                bytesSinceLastPosIndex = 0;
            }
            bytesSinceLastPosIndex += currentEntryBytes.length;
            currentSizeTotal += currentEntryBytes.length;
            lastRecord = record;
            if (currentSizeTotal >= maxFileSize) {
                break;
            }
        }
        if (lastRecord != null) {
            fileIndexEntry.setEndKey(lastRecord.getKey());
        }
        writeEofAndClose(fileOutputStream);
        return fileIndexEntry;
    }

    private static void writeEofAndClose(final OutputStream fileOutputStream) throws IOException {
        fileOutputStream.write(ByteBuffer.allocate(ByteUtils.SIZE_DESCRIPTOR_LENGTH).putShort(ByteUtils.END_OF_FILE).array());
        fileOutputStream.close();
    }

    private static Index.Entry createFileIndexEntry(final Conf conf, final String name) {
        Long timestampForFile;
        String fileName;
        File file;
        do {
            timestampForFile = System.currentTimeMillis();
            fileName = conf.getDataDir(name) + "/" + timestampForFile;
            file = new File(fileName);
        } while (file.exists());
        return new Index.Entry(timestampForFile, false);
    }

    private static byte[] encodeEntry(Map.Entry<byte[], byte[]> entry) {
        return encodeResult(entry.getKey(), entry.getValue());
    }

    private static byte[] encodeResult(final byte[] key, final byte[] value) {
        int valueLength = value == null
                ? 0
                : value.length;
        int sizeOfEntry = ByteUtils.SIZE_DESCRIPTOR_LENGTH + key.length + ByteUtils.SIZE_DESCRIPTOR_LENGTH + valueLength;
        ByteBuffer byteBuffer = ByteBuffer.allocate(sizeOfEntry);
        byteBuffer.putShort((short) key.length);
        byteBuffer.put(key);
        if (value == null) {
            byteBuffer.putShort(ByteUtils.DELETED_VALUE);
        } else {
            byteBuffer.putShort((short) value.length);
            byteBuffer.put(value);
        }
        return byteBuffer.array();
    }
}
