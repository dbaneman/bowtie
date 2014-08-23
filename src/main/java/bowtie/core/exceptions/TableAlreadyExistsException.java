package bowtie.core.exceptions;

/**
 * Thrown if the user attempts to create a table that already exists.
 */
public class TableAlreadyExistsException extends RuntimeException {
    public TableAlreadyExistsException(String tableName) {
        super("Table '" + tableName + "' already exists!");
    }
}
