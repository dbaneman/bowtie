package bowtie.core.internal;

import bowtie.core.Table;

import java.io.IOException;

/**
 * Created by dan on 8/10/14.
 */
public class MajorCompactionTest extends CompactionTestBase {

    @Override
    protected void doCompaction(Table table) throws IOException {
        table.compactMajor();
    }

}
