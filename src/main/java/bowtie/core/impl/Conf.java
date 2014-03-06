package bowtie.core.impl;

import bowtie.core.api.external.IConf;
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

    public static final String MAX_MEM_STORE_SIZE = "bowtie.mem-table-max-size-in-bytes";
    public static final String HOME_DIR = "bowtie.home-dir";
    public static final String BYTES_BETWEEN_INDEXED_KEYS = "bowtie.bytes-between-indexed-keys";
    public static final String TABLE_NAME = "bowtie.table-name";

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
        this(ConfigFactory.load());
    }

    @Override
    public String getDataDir() {
        return getString(HOME_DIR) + getString(TABLE_NAME) + "/";
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

    @Override
    public Boolean getBoolean(String parameter) {
        cacheIfNotCached(parameter);
        return (Boolean) parameterCache.get(parameter);
    }

    private void cacheIfNotCached(String parameter) {
        if (!parameterCache.containsKey(parameter) && config.hasPath(parameter)) {
            parameterCache.put(parameter, config.getValue(parameter).unwrapped());
        }
    }

}
