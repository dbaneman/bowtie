package bowtie.core.impl;

import bowtie.core.api.internal.IFileIndex;
import bowtie.core.api.internal.IFileIndexEntry;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 2/8/14
 * Time: 6:14 PM
 * To change this template use File | Settings | File Templates.
 */
public class FileIndex implements IFileIndex {

    @Override
    public void addEntry(IFileIndexEntry entry) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Iterable<String> getFilesPossiblyContainingKey(byte[] key) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Iterable<String> getFilesPossiblyContainingKeyRange(byte[] inclStart, byte[] exclEnd) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public long getClosestPositionBeforeOrAtKey(byte[] inclStart) {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
