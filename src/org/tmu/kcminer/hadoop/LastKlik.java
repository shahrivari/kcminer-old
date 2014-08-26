package org.tmu.kcminer.hadoop;

import com.carrotsearch.hppc.cursors.LongCursor;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.tmu.kcminer.KlikState;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by Saeed on 8/25/14.
 */
public class LastKlik {
    public static class Map extends Mapper<LongWritable, LongArrayWritable, Text, NullWritable> {
        public void map(LongWritable key, LongArrayWritable value, Context context) throws IOException, InterruptedException {
            KlikState state = KlikState.fromLongs(value.array);
            long[] clique = Arrays.copyOf(state.subgraph, state.subgraph.length + 1);
            String clique_len = Integer.toString(clique.length);
            for (LongCursor w : state.extension) {
                clique[clique.length - 1] = w.value;
                context.write(new Text(arrayToString(clique)), NullWritable.get());
            }
            context.getCounter("#Cliques", clique_len).increment(state.extension.size());
        }

        public static String arrayToString(long[] array) {
            if (array == null || array.length == 0)
                return "";
            StringBuilder builder = new StringBuilder();
            for (long l : array)
                builder.append(l).append("\t");
            builder.setLength(builder.length() - 1);
            return builder.toString();
        }

    }
}
