package bowtie.core;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 1/27/14
 * Time: 9:02 PM
 * To change this template use File | Settings | File Templates.
 */
public class FileSysTable implements ITableReader {

    public FileSysTable(final IConf conf) {
        //To change body of created methods use File | Settings | File Templates.
    }

    @Override
    public IConf getConf() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Iterable<IResult> scan(byte[] inclStart, byte[] exclStop) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public IResult get(byte[] key) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

}
