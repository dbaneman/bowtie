package bowtie.core.impl;

import bowtie.core.api.internal.IConf;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 1/27/14
 * Time: 8:52 PM
 * To change this template use File | Settings | File Templates.
 */
public class Conf implements IConf {

    public static final String MAX_MEM_STORE_SIZE = "bowtie.mem-table-max-size-in-mb";
    public static final String DATA_DIR = "bowtie.data-dir";
    public static final String HOME_DIR = "bowtie.home-dir";
    public static final String BYTES_BETWEEN_INDEXED_KEYS = "bowtie.bytes-between-indexed-keys";

    private final Config config;
    private final Map<String, Object> parameterCache;

    public Conf(Config config) {
        this.config = config;
        parameterCache = new HashMap<String, Object>();
    }

    public Conf(String confFileLocation) {
        this(ConfigFactory.load(confFileLocation));
    }

    public Conf() {
        this((Config) null);
    }

    @Override
    public String getString(String parameter) {
        cacheIfNotCached(parameter);
        return (String) parameterCache.get(parameter);
    }

    @Override
    public Integer getInt(String parameter) {
        cacheIfNotCached(parameter);
        return (Integer) parameterCache.get(parameter);
    }

    @Override
    public Long getLong(String parameter) {
        cacheIfNotCached(parameter);
        return (Long) parameterCache.get(parameter);
    }

    private void cacheIfNotCached(String parameter) {
        if (!parameterCache.containsKey(parameter) && config.hasPath(parameter)) {
            parameterCache.put(parameter, config.getValue(parameter).unwrapped());
        }
    }

}
