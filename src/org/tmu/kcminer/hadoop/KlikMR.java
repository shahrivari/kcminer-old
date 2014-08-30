package org.tmu.kcminer.hadoop;

import com.carrotsearch.hppc.cursors.LongCursor;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.tmu.kcminer.KlikState;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by Saeed on 8/26/14.
 */
public class KlikMR {
    static boolean maximal = false;
    static int lower = 0;
    public static final int termination = 1;

    public static class Map extends Mapper<LongWritable, LongArrayWritable, LongWritable, LongArrayWritable> {
        LongWritable l = new LongWritable();

        public void map(LongWritable key, LongArrayWritable value, Context context) throws IOException, InterruptedException {
            if (value.termination == GraphLayer.termination) {
                context.write(key, value);
                context.getCounter("Graph", "#Nodes").increment(1);
                return;
            }
            KlikState state = KlikState.fromLongs(value.array);
            for (LongCursor w : state.extension) {
                l.set(w.value);
                context.write(l, value);
            }
        }
    }

    public static class Reduce extends Reducer<LongWritable, LongArrayWritable, LongWritable, LongArrayWritable> {

        public void reduce(LongWritable key, Iterable<LongArrayWritable> values, Context context) throws IOException, InterruptedException {
            long w = key.get();
            long[] w_neighbors = null;
            ArrayList<long[]> list = new ArrayList<long[]>();

            for (LongArrayWritable v : values) {
                if (v.termination == GraphLayer.termination) {
                    w_neighbors = v.array;
                    continue;
                }
                if (w_neighbors == null && v.termination != GraphLayer.termination) {
                    list.add(v.array.clone());
                    continue;
                }
                KlikState state = KlikState.fromLongs(v.array);
                KlikState new_state;
                if (maximal)
                    new_state = state.expandMax(w, w_neighbors);
                else
                    new_state = state.expandFixed(w, w_neighbors);
//                    if (new_state.subgraph.length + new_state.extension.elementsCount >= lower)
//                        stack.add(new_state);
                context.write(key, new LongArrayWritable(new_state.toLongs(), termination));
                context.getCounter("#States", Integer.toString(new_state.subgraph.length)).increment(1);
            }

            for (long[] v : list) {
                KlikState state = KlikState.fromLongs(v);
                KlikState new_state;
                if (maximal)
                    new_state = state.expandMax(w, w_neighbors);
                else
                    new_state = state.expandFixed(w, w_neighbors);
//                    if (new_state.subgraph.length + new_state.extension.elementsCount >= lower)
//                        stack.add(new_state);
                context.write(key, new LongArrayWritable(new_state.toLongs(), termination));
                context.getCounter("States", Integer.toString(new_state.subgraph.length)).increment(1);
            }

        }
    }
}
