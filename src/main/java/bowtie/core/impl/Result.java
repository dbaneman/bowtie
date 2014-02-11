package bowtie.core.impl;

import bowtie.core.api.external.IResult;

/**
 * Created with IntelliJ IDEA.
 * User: dan
 * Date: 1/27/14
 * Time: 10:59 PM
 * To change this template use File | Settings | File Templates.
 */
public class Result implements IResult {
    private final byte[] key;
    private final byte[] value;

    public Result(final byte[] key, final byte[] value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public byte[] getKey() {
        return key;
    }

    @Override
    public byte[] getValue() {
        return value;
    }
}
