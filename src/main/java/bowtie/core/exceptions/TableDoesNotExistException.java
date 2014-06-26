package bowtie.core.exceptions;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 6/26/14
 * Time: 12:33 AM
 * To change this template use File | Settings | File Templates.
 */
public class TableDoesNotExistException extends RuntimeException {
    public TableDoesNotExistException(String name) {
        super("Table '" + name + "' does not exist!");
    }
}
