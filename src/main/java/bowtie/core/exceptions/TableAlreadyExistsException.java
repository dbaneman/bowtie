package bowtie.core.exceptions;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 6/26/14
 * Time: 12:20 AM
 * To change this template use File | Settings | File Templates.
 */
public class TableAlreadyExistsException extends RuntimeException {
    public TableAlreadyExistsException(String tableName) {
        super("Table '" + tableName + "' already exists!");
    }
}
