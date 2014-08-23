package bowtie.core;

import bowtie.core.internal.Conf;
import bowtie.core.internal.ResultImpl;
import bowtie.core.internal.TableImpl;

import java.io.IOException;

/**
 * Factory for creating Table and Result objects.
 */
public class BowtieFactory {

    /**
     * Creates a new table object with the supplied configuration.
     * @param tableName table name
     * @param confFileLocation absolute path of custom config file
     * @return
     * @throws IOException
     */
    public static Table newTable(String tableName, String confFileLocation) throws IOException {
        return new TableImpl(new Conf(confFileLocation), tableName);
    }

    /**
     * Creates a new table object with the default configuration.
     * @param tableName table name
     * @return
     * @throws IOException
     */
    public static Table newTable(String tableName) throws IOException {
        return new TableImpl(new Conf(), tableName);
    }

    /**
     * Creates a new result object.
     * @param key
     * @param value
     * @return
     */
    public static Result wrapResult(final byte[] key, final byte[] value) {
        return ResultImpl.wrap(key, value);
    }

}
