package bowtie.core.exceptions;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 6/29/14
 * Time: 3:50 PM
 * To change this template use File | Settings | File Templates.
 */
public class ClosedTableException extends RuntimeException {
    public ClosedTableException(String name) {
        super("Attempted to perform operation on closed table '" + name + "'.");
    }
}
