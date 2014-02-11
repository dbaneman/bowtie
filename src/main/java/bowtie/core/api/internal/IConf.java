package bowtie.core.api.internal;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 1/27/14
 * Time: 8:49 PM
 * To change this template use File | Settings | File Templates.
 */
public interface IConf {
    String getString(String parameter);
    Integer getInt(String parameter);
    Long getLong(String parameter);
}
