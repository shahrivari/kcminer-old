package org.tmu.kcminer.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Saeed on 8/25/14.
 */
public class Distribute {
    public static class Map extends Mapper<LongWritable, LongArrayWritable, LongWritable, LongArrayWritable> {
        public void map(LongWritable key, LongArrayWritable value, Context context) throws IOException, InterruptedException {
            long[] array = new long[value.array.length + 1];
            array[0] = key.get();
            System.arraycopy(value.array, 0, array, 1, value.array.length);
            for (long l : value.array)
                context.write(key, new LongArrayWritable(array, KlikMR.termination));
        }

    }
}
