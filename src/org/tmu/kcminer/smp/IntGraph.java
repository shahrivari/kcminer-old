package org.tmu.kcminer.smp;


import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.IntOpenHashSet;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.carrotsearch.hppc.cursors.IntObjectCursor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

/**
 * Created by Saeed on 8/1/14.
 */
public class IntGraph {
    IntOpenHashSet vertex_set = new IntOpenHashSet();
    int[] vertices;
    IntObjectOpenHashMap<int[]> adjArray = new IntObjectOpenHashMap<int[]>();
    IntObjectOpenHashMap<IntOpenHashSet> adjSet = new IntObjectOpenHashMap<IntOpenHashSet>();

    private void addEdge(int v, int w) {
        vertex_set.add(v, w);
        if (!adjSet.containsKey(v))
            adjSet.put(v, new IntOpenHashSet());
        if (!adjSet.containsKey(w))
            adjSet.put(w, new IntOpenHashSet());

        adjSet.get(v).add(w);
        adjSet.get(w).add(v);
    }

    private void update() {
        adjArray = new IntObjectOpenHashMap<int[]>(adjSet.size());
        vertices = vertex_set.toArray();
        Arrays.sort(vertices);
        for (IntCursor cursor : vertex_set) {
            adjArray.put(cursor.value, adjSet.get(cursor.value).toArray());
            Arrays.sort(adjArray.get(cursor.value));
            adjSet.get(cursor.value);
        }
    }

    public int[] getNeighbors(int v) {
        return adjArray.get(v);
    }


    public void buildFromEdgeListFile(String path) throws IOException {
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
            int src = Integer.parseInt(tokens[0]);
            int dest = Integer.parseInt(tokens[1]);
            addEdge(src, dest);
        }
        update();
        br.close();
    }

    public String getInfo() {
        String info = "#Nodes: " + String.format("%,d", vertices.length) + "\n";
        long edges = 0;
        for (IntObjectCursor<int[]> x : adjArray)
            edges += x.value.length;
        info += "#Edges: " + String.format("%,d", edges) + "\n";
        info += "AVG(degree): " + String.format("%.2f", edges / (double) vertices.length);
        return info;
    }


}
