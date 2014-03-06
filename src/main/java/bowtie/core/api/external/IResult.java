package bowtie.core.api.external;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 1/27/14
 * Time: 10:58 PM
 * To change this template use File | Settings | File Templates.
 */
public interface IResult {
    byte[] getKey();
    byte[] getValue();
    boolean noVal();
}
