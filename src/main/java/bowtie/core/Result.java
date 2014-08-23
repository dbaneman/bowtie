package bowtie.core;

import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 1/27/14
 * Time: 10:58 PM
 * To change this template use File | Settings | File Templates.
 */
public interface Result extends Map.Entry<byte[], byte[]> {
    @Override
    byte[] getKey();

    @Override
    byte[] getValue();

    boolean noVal();
}
