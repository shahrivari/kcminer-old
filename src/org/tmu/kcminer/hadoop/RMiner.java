package org.tmu.kcminer.hadoop;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.tmu.kcminer.Graph;

import java.io.IOException;

/**
 * Created by Saeed on 9/21/14.
 */
public class RMiner {
    public static class Map extends Mapper<LongWritable, Text, Text, NullWritable> {
        Graph graph = null;
        int k = 0;
        int lower = 0;
        boolean maximal = false;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            if (graph == null) {
                Path pt = new Path(context.getConfiguration().get("graph_path"));
                FileSystem fs = FileSystem.get(context.getConfiguration());
                graph = Graph.readFromStream(fs.open(pt));
                context.getCounter(Counters.GraphLoads).increment(1);
            }
            if (k == 0) {
                k = context.getConfiguration().getInt("k", 0);
                lower = k;
            }
            if (k == 0)
                throw new IllegalArgumentException("Bad clique size!");
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                context.getCounter(Counters.GraphNodes).increment(1);
                long[] counts = new long[k + 1];
                context.write(new Text(key.toString() + "->" + value.toString()), NullWritable.get());
                context.write(new Text("Val is" + value.toString()), NullWritable.get());
                context.write(new Text("Len  " + value.toString().length()), NullWritable.get());
                context.write(new Text("LenT  " + value.getLength()), NullWritable.get());
                for (int i = 0; i < value.toString().length(); i++)
                    context.write(new Text("Char @  " + i + value.toString().charAt(i)), NullWritable.get());


                long v = Long.parseLong(value.toString());

//            Stack<KlikState> stack = new Stack<KlikState>();
//            stack.add(new KlikState(v, graph.getNeighbors(v)));
//            StringBuilder builder = new StringBuilder();
//            while (!stack.isEmpty()) {
//                KlikState state = stack.pop();
//                if (state.subgraph.length == k - 1) {
//                    counts[k] += state.extension.elementsCount;
//                    for (LongCursor cursor : state.extension)
//                        builder.append(KlikState.cliqueToString(state.getClique(cursor.value))).append("\n");
//                }
//                if (state.subgraph.length >= lower){
//                    counts[state.subgraph.length]++;
//                    if (!maximal)
//                        builder.append(KlikState.cliqueToString(state.subgraph)).append("\n");
//                    else if (state.extension.isEmpty() && state.tabu.isEmpty())
//                        builder.append(KlikState.cliqueToString(state.subgraph)).append("\n");
//
//                }
//                if (state.subgraph.length == k - 1)
//                    continue;
//
//                for (LongCursor w : state.extension) {
//                    KlikState new_state;
//                    if (maximal)
//                        new_state = state.expandMax(w.value, graph.getNeighbors(w.value));
//                    else
//                        new_state = state.expandFixed(w.value, graph.getNeighbors(w.value));
//                    if (new_state.subgraph.length + new_state.extension.elementsCount >= lower)
//                        stack.add(new_state);
//                }
//            }
//            context.write(new Text(builder.toString()),NullWritable.get());
//            for(int i=0;i<counts.length;i++)
//                if(counts[i]>0)
//                    context.getCounter("Cliques",Integer.toString(i)).increment(counts[i]);
            } catch (Exception exp) {
                context.getCounter(Counters.Exception).increment(1);
                System.out.println("Node" + value.toString());
                exp.printStackTrace();
            }
        }

    }

}


