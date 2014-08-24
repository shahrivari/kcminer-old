package org.tmu.kcminer;

import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.carrotsearch.hppc.LongOpenHashSet;
import com.carrotsearch.hppc.cursors.LongObjectCursor;
import com.google.common.io.Files;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.Arrays;

/**
 * Created by Saeed on 8/8/14.
 */
public class Graph {
    LongOpenHashSet vertex_set = new LongOpenHashSet();
    public long[] vertices;
    LongObjectOpenHashMap<long[]> adjArray = new LongObjectOpenHashMap<long[]>();
    LongObjectOpenHashMap<LongOpenHashSet> adjSet = new LongObjectOpenHashMap<LongOpenHashSet>();

    private void Graph() {
    }

    private void addEdge(long v, long w) {
        vertex_set.add(v, w);
        if (!adjSet.containsKey(v))
            adjSet.put(v, new LongOpenHashSet());
        if (!adjSet.containsKey(w))
            adjSet.put(w, new LongOpenHashSet());

        adjSet.get(v).add(w);
        adjSet.get(w).add(v);
    }

    private void update() {
        adjArray = new LongObjectOpenHashMap<long[]>(adjSet.size());
        vertices = vertex_set.toArray();
        Arrays.sort(vertices);
        for (long cursor : vertices) {
            adjArray.put(cursor, adjSet.get(cursor).toArray());
            Arrays.sort(adjArray.get(cursor));
            adjSet.remove(cursor);
        }

    }

    public long[] getNeighbors(long v) {
        return adjArray.get(v);
    }

    public static Graph buildFromEdgeListFile(String path) throws IOException {
        Graph g = new Graph();
        BufferedReader br = new BufferedReader(new FileReader(path));
        String line;
        while ((line = br.readLine()) != null) {
            if (line.isEmpty())
                continue;
            if (line.startsWith("#")) {
                System.err.printf("Skipped a line: [%s]\n", line);
                continue;
            }
            String[] tokens = line.split("\\s+");
            if (tokens.length < 2) {
                System.err.printf("Skipped a line: [%s]\n", line);
                continue;
            }
            long src = Long.parseLong(tokens[0]);
            long dest = Long.parseLong(tokens[1]);
            g.addEdge(src, dest);
        }
        g.update();
        br.close();
        return g;
    }

    public String getInfo() {
        String info = "#Nodes: " + String.format("%,d", vertices.length) + "\n";
        long edges = 0;
        for (LongObjectCursor<long[]> x : adjArray)
            edges += x.value.length;
        info += "#Edges: " + String.format("%,d", edges) + "\n";
        info += "AVG(degree): " + String.format("%.2f", edges / (double) vertices.length);
        return info;
    }



    public static void layEdgeListToDisk(String in_path, String out_dir, int bucket_count) throws IOException {
        //clear the directory
        if (new File(out_dir).exists())
            Util.deleteDirectory(new File(out_dir));
        new File(out_dir).mkdir();
        new File(out_dir + "/tmp/").mkdir();
        new File(out_dir + "/graph/").mkdir();

        DataOutputStream[] ostreams = new DataOutputStream[bucket_count];
        for (int i = 0; i < bucket_count; i++)
            ostreams[i] = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(out_dir + "/tmp/" + String.valueOf(i) + ".bin"), 512 * 1024));

        BufferedReader br = new BufferedReader(new FileReader(in_path));
        String line;
        while ((line = br.readLine()) != null) {
            if (line.isEmpty())
                continue;
            if (line.startsWith("#")) {
                System.err.printf("Skipped a line: [%s]\n", line);
                continue;
            }
            String[] tokens = line.split("\\s+");
            if (tokens.length < 2) {
                System.err.printf("Skipped a line: [%s]\n", line);
                continue;
            }
            long src = Long.parseLong(tokens[0]);
            long dest = Long.parseLong(tokens[1]);

            int bucket = Util.longToBucket(src, bucket_count);
            if (bucket < 0) {
                System.out.printf("%d %d\n", src, bucket);
                Util.longToBucket(src, bucket_count);
            }
            ostreams[bucket].writeLong(src);
            ostreams[bucket].writeLong(dest);
        }

        for (DataOutputStream s : ostreams)
            s.close();

        for (int i = 0; i < bucket_count; i++)
            ostreams[i] = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(out_dir + "/graph/" + String.valueOf(i) + ".gseg"), 512 * 1024));


        for (int i = 0; i < bucket_count; i++) {
            byte[] bb = Files.toByteArray(new File(out_dir + "/tmp/" + String.valueOf(i) + ".bin"));
            LongBuffer buffer = ByteBuffer.wrap(bb).asLongBuffer();
            Graph g = new Graph();
            while (buffer.hasRemaining()) {
                long src = buffer.get();
                long dest = buffer.get();
                g.vertex_set.add(src);
                if (!g.adjSet.containsKey(src))
                    g.adjSet.put(src, new LongOpenHashSet());
                g.adjSet.get(src).add(dest);
            }
            g.update();
            for (LongObjectCursor<long[]> cur : g.adjArray) {
                ostreams[i].writeLong(cur.key);
                ostreams[i].writeInt(cur.value.length);
                for (long l : cur.value)
                    ostreams[i].writeLong(l);
            }
            ostreams[i].close();
            System.out.println("Done: " + out_dir + "/graph/" + String.valueOf(i) + ".gseg");
            new File(out_dir + "/tmp/" + String.valueOf(i) + ".bin").delete();
        }
    }

    public static Graph loadFromSegment(String root_dir, int number) throws IOException {
        Graph g = new Graph();
        byte[] bb = Files.toByteArray(new File(root_dir + "/graph/" + String.valueOf(number) + ".gseg"));
        ByteBuffer buffer = ByteBuffer.wrap(bb);
        while (buffer.hasRemaining()) {
            long src = buffer.getLong();
            int count = buffer.getInt();
            g.vertex_set.add(src);
            long[] array = new long[count];
            for (int i = 0; i < count; i++)
                array[i] = buffer.getLong();
            g.adjArray.put(src, array);
        }
        g.vertices = g.vertex_set.toArray();
        Arrays.sort(g.vertices);
        return g;
    }

}
