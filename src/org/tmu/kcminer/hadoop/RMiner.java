package org.tmu.kcminer.hadoop;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.tmu.kcminer.Graph;
import org.tmu.kcminer.KlikState;

import java.io.IOException;
import java.util.Stack;

/**
 * Created by Saeed on 9/21/14.
 */
public class RMiner {
    public static class Map extends Mapper<LongWritable, Text, Text, NullWritable> {
        Graph graph = null;
        int k = 0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            if (graph == null) {
                Path pt = new Path(context.getConfiguration().get("graph_path"));
                FileSystem fs = FileSystem.get(context.getConfiguration());
                graph = Graph.readFromStream(fs.open(pt));
                context.getCounter(Counters.GraphLoads).increment(1);
            }
            if (k == 0)
                k = context.getConfiguration().getInt("k", 0);
            if (k == 0)
                throw new IllegalArgumentException("Bad clique size!");
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.getCounter(Counters.GraphNodes).increment(1);
            long v = key.get();
            long count = 0;
            Stack<KlikState> stack = new Stack<KlikState>();
            stack.add(new KlikState(v, graph.getNeighbors(v)));
            while (!stack.isEmpty()) {
                KlikState state = stack.pop();
                if (state.subgraph.length == k - 1) {
                    count += state.extension.elementsCount;
//                    if (writer != null)
//                        for (LongCursor cursor : state.extension)
//                            builder.append(cliqueToString(state.getClique(cursor.value))).append("\n");
                }
                if (state.subgraph.length >= k)
//                    if (!maximal) {
//                        counter.getAndIncrement();
//                        if (writer != null)
//                            builder.append(cliqueToString(state.subgraph)).append("\n");
//                    } else if (state.extension.isEmpty() && state.tabu.isEmpty()) {
//                        counter.getAndIncrement();
//                        if (writer != null)
//                            builder.append(cliqueToString(state.subgraph)).append("\n");
//                    }
                    if (state.subgraph.length == k - 1)
                        continue;
//                for (LongCursor w : state.extension) {
//                    KlikState new_state;
//                    if (maximal)
//                        new_state = state.expandMax(w.value, g.getNeighbors(w.value));
//                    else
//                        new_state = state.expandFixed(w.value, g.getNeighbors(w.value));
//                    if (new_state.subgraph.length + new_state.extension.elementsCount >= lower)
//                        stack.add(new_state);
//                }
//                if (builder.length() > flushLimit && writer != null)
//                    flush(builder);
            }
        }

    }

}


