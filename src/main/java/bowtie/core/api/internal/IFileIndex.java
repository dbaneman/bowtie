package bowtie.core.api.internal;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 2/8/14
 * Time: 6:04 PM
 *
 * This is a sorted map containing the start and end keys of every file.
 */
public interface IFileIndex {
    void addEntry(IFileIndexEntry entry);
    Iterable<IFileIndexEntry> getFilesPossiblyContainingKey(byte[] key);
    Iterable<IFileIndexEntry> getFilesPossiblyContainingKeyRange(byte[] inclStart, byte[] exclEnd);
    long getClosestPositionBeforeOrAtKey(byte[] inclStart, IFileIndexEntry possibleHit);
}
