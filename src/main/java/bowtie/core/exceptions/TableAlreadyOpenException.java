package bowtie.core.exceptions;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 6/29/14
 * Time: 5:28 PM
 * To change this template use File | Settings | File Templates.
 */
public class TableAlreadyOpenException extends RuntimeException {
    public TableAlreadyOpenException(String name) {
        super("Table '" + name + "' is already open.");
    }
}
