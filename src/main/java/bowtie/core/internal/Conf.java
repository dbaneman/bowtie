package bowtie.core.internal;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 1/27/14
 * Time: 8:52 PM
 * To change this template use File | Settings | File Templates.
 */
public class Conf {
    public static final String HOME_DIR = "bowtie.home-dir";
    public static final String MAX_MEM_STORE_SIZE = "bowtie.mem-table-max-bytes";
    public static final String BYTES_BETWEEN_INDEXED_KEYS = "bowtie.bytes-between-indexed-keys";
    public static final String MAX_DATA_FILE_SIZE = "bowtie.data-file-max-bytes";

    private final Config config;
    private final Map<String, Object> parameterCache;

    private Conf(Config config) {
        this.config = config;
        parameterCache = new HashMap<String, Object>();
    }

    /**
     * Create a bowtie configuration from the file with the given path.
     * @param confFileLocation
     */
    public Conf(String confFileLocation) {
        this(ConfigFactory.parseFile(new File(confFileLocation)).withFallback(ConfigFactory.load()).resolve());
    }

    /**
     * Create a bowtie configuration using the default parameters.
     */
    public Conf() {
        this(ConfigFactory.load());
    }

    public String getDataDir(String tableName) {
        return getString(HOME_DIR) + tableName + "/";
    }

    public String getString(String parameter) {
        cacheIfNotCached(parameter);
        return (String) parameterCache.get(parameter);
    }

    public Integer getInt(String parameter) {
        cacheIfNotCached(parameter);
        return (Integer) parameterCache.get(parameter);
    }

    public Long getLong(String parameter) {
        cacheIfNotCached(parameter);
        final Object ret = parameterCache.get(parameter);
        return ret instanceof Integer
                ? (Long) ((long) ((Integer) ret))
                : (Long) ret;
    }

    public Boolean getBoolean(String parameter) {
        cacheIfNotCached(parameter);
        return (Boolean) parameterCache.get(parameter);
    }

    public void set(String parameter, Object value) {
        parameterCache.put(parameter, value);
    }

    private void cacheIfNotCached(String parameter) {
        if (!parameterCache.containsKey(parameter) && config.hasPath(parameter)) {
            parameterCache.put(parameter, config.getValue(parameter).unwrapped());
        }
    }

}
