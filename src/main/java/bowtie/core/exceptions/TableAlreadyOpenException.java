package bowtie.core.exceptions;

/**
 * Thrown if the user attempts to open a table that is already open.
 */
public class TableAlreadyOpenException extends RuntimeException {
    public TableAlreadyOpenException(String name) {
        super("Table '" + name + "' is already open.");
    }
}
