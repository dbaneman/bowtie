package bowtie.core;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 1/27/14
 * Time: 8:52 PM
 * To change this template use File | Settings | File Templates.
 */
public class Conf implements IConf {
    public static final String DEFAULT_CONF_APPLICATION_FILE_LOCATION = "/opt/bowtie/conf/bowtie.conf";

    public static final String MAX_MEM_STORE_SIZE = "bowtie.mem-table-max-size-in-mb";

    private final Config config;

    public Conf(String confFileLocation) {
        config = ConfigFactory.load(confFileLocation);
    }

    public Conf() {
        this(DEFAULT_CONF_APPLICATION_FILE_LOCATION);
    }

    @Override
    public String getString(String parameter) {
        return config.getString(parameter);
    }

    @Override
    public Integer getInt(String parameter) {
        return config.getInt(parameter);
    }

    @Override
    public Long getLong(String parameter) {
        return config.getLong(parameter);
    }
}
