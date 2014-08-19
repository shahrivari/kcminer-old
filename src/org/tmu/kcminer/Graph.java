package org.tmu.kcminer;

import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.carrotsearch.hppc.LongOpenHashSet;
import com.carrotsearch.hppc.cursors.LongObjectCursor;

import java.io.*;
import java.util.Arrays;

/**
 * Created by Saeed on 8/8/14.
 */
public class Graph {
    LongOpenHashSet vertex_set = new LongOpenHashSet();
    public long[] vertices;
    LongObjectOpenHashMap<long[]> adjArray = new LongObjectOpenHashMap<long[]>();
    LongObjectOpenHashMap<LongOpenHashSet> adjSet = new LongObjectOpenHashMap<LongOpenHashSet>();

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

    public void loadFromEdgeListFile(String path) throws IOException {
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
            addEdge(src, dest);
        }
        update();
        br.close();
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

    private static void delete(File f) throws IOException {
        if (f.isDirectory()) {
            for (File c : f.listFiles())
                delete(c);
        }
        if (!f.delete())
            throw new FileNotFoundException("Failed to delete file: " + f);
    }

    public static void layEdgeListToDisk(String in_path, String out_dir, int bucket_count) throws IOException {
        //clear the directory
        if (new File(out_dir).exists())
            delete(new File(out_dir));
        new File(out_dir).mkdir();

        DataOutputStream[] ostreams = new DataOutputStream[bucket_count];
        for (int i = 0; i < bucket_count; i++)
            ostreams[i] = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(out_dir + "/" + String.valueOf(i)), 512 * 1024));


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


        }
    }

}
