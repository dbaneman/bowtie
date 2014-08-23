package bowtie.core.exceptions;

/**
 * Thrown if user attempts to write to, read from, or close and table that is closed.
 */
public class ClosedTableException extends RuntimeException {
    public ClosedTableException(String name) {
        super("Attempted to perform operation on closed table '" + name + "'.");
    }
}
