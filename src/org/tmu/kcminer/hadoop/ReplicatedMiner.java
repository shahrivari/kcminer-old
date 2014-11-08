package org.tmu.kcminer.hadoop;

import com.carrotsearch.hppc.cursors.LongCursor;
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
public class ReplicatedMiner {
    public static class Map extends Mapper<LongWritable, Text, Text, NullWritable> {
        Graph graph = null;
        int k = 0;
        int lower = 0;
        boolean maximal = false;
        boolean dumpCliques = true;

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
                lower = context.getConfiguration().getInt("lower", 0);
            }
            if (k == 0 || lower == 0)
                throw new IllegalArgumentException("Bad clique size!");

            maximal = context.getConfiguration().getBoolean("maximal", false);
            dumpCliques = context.getConfiguration().getBoolean("dump", true);
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                //context.getCounter(Counters.GraphNodes).increment(1);
                long[] counts = new long[k + 1];
                Stack<KlikState> stack = new Stack<KlikState>();

                //long v = Long.parseLong(value.toString());
                //stack.add(new KlikState(v, graph.getNeighbors(v)));
                if (value.toString().contains(",")) {
                    String[] tokens = value.toString().split(",");
                    if (tokens.length != 2)
                        throw new IllegalArgumentException("Expected two tokens!");
                    long v = Long.parseLong(tokens[0]);
                    long w1 = Long.parseLong(tokens[1]);
                    KlikState state = new KlikState(v, graph.getNeighbors(v));
                    //System.out.print("(" + v + "," + w1 + ")->(");
                    for (LongCursor w : state.extension) {
                        if (w1 != w.value)
                            continue;

                        KlikState new_state;
                        if (maximal)
                            new_state = state.expandMax(w.value, graph.getNeighbors(w.value));
                        else
                            new_state = state.expandFixed(w.value, graph.getNeighbors(w.value));

                        stack.add(new_state);
                        //System.out.println(graph.getNeighbors(v).length + "," + graph.getNeighbors(w1).length + ")");
                        context.getCounter("Input", "Edges").increment(1);
                        break;
                    }
                } else {
                    long v = Long.parseLong(value.toString());
                    //System.out.println(v + "->" + graph.getNeighbors(v).length);
                    context.getCounter("Input", "Nodes").increment(1);
                    stack.add(new KlikState(v, graph.getNeighbors(v)));
                }


                if (dumpCliques) {
                    StringBuilder builder = new StringBuilder();
                    while (!stack.isEmpty()) {
                        KlikState state = stack.pop();
                        if (builder.length() > 1024 * 1024) {
                            context.write(new Text(builder.toString()), NullWritable.get());
                            builder.setLength(0);
                        }
                        if (state.subgraph.length == k - 1) {
                            counts[k] += state.extension.elementsCount;
                            for (LongCursor cursor : state.extension)
                                builder.append(KlikState.cliqueToString(state.getClique(cursor.value))).append("\n");
                        }
                        if (state.subgraph.length >= lower) {
                            if (!maximal) {
                                builder.append(KlikState.cliqueToString(state.subgraph)).append("\n");
                                counts[state.subgraph.length]++;
                            } else if (state.extension.isEmpty() && state.tabu.isEmpty()) {
                                builder.append(KlikState.cliqueToString(state.subgraph)).append("\n");
                                counts[state.subgraph.length]++;
                            }
                        }
                        if (state.subgraph.length == k - 1)
                            continue;

                        for (LongCursor w : state.extension) {
                            KlikState new_state;
                            if (maximal)
                                new_state = state.expandMax(w.value, graph.getNeighbors(w.value));
                            else
                                new_state = state.expandFixed(w.value, graph.getNeighbors(w.value));
                            if (new_state.subgraph.length + new_state.extension.elementsCount >= lower)
                                stack.add(new_state);
                        }
                    }
                    context.write(new Text(builder.toString()), NullWritable.get());
                } else {
                    while (!stack.isEmpty()) {
                        KlikState state = stack.pop();
                        if (state.subgraph.length == k - 1)
                            counts[k] += state.extension.elementsCount;

                        if (state.subgraph.length >= lower)
                            if (!maximal)
                                counts[state.subgraph.length]++;
                            else if (state.extension.isEmpty() && state.tabu.isEmpty())
                                counts[state.subgraph.length]++;

                        if (state.subgraph.length == k - 1)
                            continue;

                        for (LongCursor w : state.extension) {
                            KlikState new_state;
                            if (maximal)
                                new_state = state.expandMax(w.value, graph.getNeighbors(w.value));
                            else
                                new_state = state.expandFixed(w.value, graph.getNeighbors(w.value));
                            if (new_state.subgraph.length + new_state.extension.elementsCount >= lower)
                                stack.add(new_state);
                        }
                    }
                }

                for (int i = 0; i < counts.length; i++)
                    if (counts[i] > 0) {
                        context.getCounter("Cliques", Integer.toString(i)).increment(counts[i]);
                        context.getCounter("Cliques", "ALL").increment(counts[i]);
                    }
            } catch (Exception exp) {
                context.getCounter(Counters.Exception).increment(1);
                System.out.println("Node" + value.toString());
                exp.printStackTrace();
            }
        }

    }

}


