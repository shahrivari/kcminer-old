package org.tmu.kcminer.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.tmu.kcminer.KlikState;

import java.io.IOException;

/**
 * Created by Saeed on 8/25/14.
 */
public class OneKlik {
    public static class Map extends Mapper<LongWritable, LongArrayWritable, LongWritable, LongArrayWritable> {
        public void map(LongWritable key, LongArrayWritable value, Context context) throws IOException, InterruptedException {
            KlikState state = new KlikState(key.get(), value.array);
            long[] array = state.toLongs();
            context.write(key, new LongArrayWritable(array, KlikMR.termination));
        }

    }
}
