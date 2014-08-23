package bowtie.core.exceptions;

/**
 * Thrown if the user attempts to write to, read from, open, close, or drop a table that hasn't been created.
 */
public class TableDoesNotExistException extends RuntimeException {
    public TableDoesNotExistException(String name) {
        super("Table '" + name + "' does not exist!");
    }
}
