package bowtie.core.api.internal;

import bowtie.core.api.external.IConfBacked;
import bowtie.core.api.external.IResult;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 2/10/14
 * Time: 10:40 PM
 * To change this template use File | Settings | File Templates.
 */
public interface IFileReader extends IConfBacked {
    Iterable<IResult> scanInFile(byte[] inclStart, byte[] exclStop, String possibleHit) throws IOException;
    IResult getInFile(byte[] key, String possibleHit) throws IOException;
}
