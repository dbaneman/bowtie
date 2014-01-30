package bowtie.core;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 1/27/14
 * Time: 9:14 PM
 * To change this template use File | Settings | File Templates.
 */
public interface ITableReader {
    IConf getConf();
    Iterable<IResult> scan(byte[] inclStart, byte[] exclStop);
    IResult get(byte[] key);
}
