package bowtie.core;

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
    Iterable<String> getFilesPossiblyContainingKeyRange(byte[] inclStart, byte[] exclEnd);
    long getClosestPositionBeforeOrAtKeyRange(byte[] inclStart);
}
