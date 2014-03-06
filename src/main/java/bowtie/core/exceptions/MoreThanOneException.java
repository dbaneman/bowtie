package bowtie.core.exceptions;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 3/5/14
 * Time: 9:58 PM
 * To change this template use File | Settings | File Templates.
 */
public class MoreThanOneException extends RuntimeException {
    public MoreThanOneException(int numberOfElements) {
        super("Expected one element but got " + numberOfElements + ".");
    }
}
