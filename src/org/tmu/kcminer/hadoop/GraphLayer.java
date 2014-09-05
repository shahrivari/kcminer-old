package org.tmu.kcminer.hadoop;

import com.carrotsearch.hppc.LongOpenHashSet;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by Saeed on 8/25/14.
 */
public class GraphLayer {
    public static final int termination = 0;

    public static class Map extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
        LongWritable src = new LongWritable();
        LongWritable dest = new LongWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("#"))
                return;
            String[] tokens = line.split("\\s+");
            if (tokens.length < 2) {
                return;
            }
            src.set(Long.parseLong(tokens[0]));
            dest.set(Long.parseLong(tokens[1]));
            context.write(src, dest);
            context.write(dest, src);
        }
    }

    public static class Reduce extends Reducer<LongWritable, LongWritable, LongWritable, LongArrayWritable> {
        public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            LongOpenHashSet set = LongOpenHashSet.newInstance();
            for (LongWritable l : values) {
                set.add(l.get());
                context.getCounter("Graph", "2x#DirectedEdges").increment(1);
            }
            context.getCounter("Graph", "#Nodes").increment(1);
            context.getCounter("Graph", "2x#Edges").increment(set.size());
            long[] array = set.toArray();
            System.out.println(Arrays.toString(array));
            Arrays.sort(array);
            context.write(key, new LongArrayWritable(array, 0));
        }
    }

}
