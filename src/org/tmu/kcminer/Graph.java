package org.tmu.kcminer;

import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.carrotsearch.hppc.LongOpenHashSet;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

/**
 * Created by Saeed on 8/8/14.
 */
public class Graph {
    LongOpenHashSet vertex_set = new LongOpenHashSet();
    long[] vertices;
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
            long src = Long.parseLong(tokens[0]);
            long dest = Long.parseLong(tokens[1]);
            addEdge(src, dest);
        }
        update();
        br.close();
    }

}
