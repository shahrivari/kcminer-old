package org.tmu.kcminer;

import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.carrotsearch.hppc.LongOpenHashSet;
import com.carrotsearch.hppc.cursors.LongObjectCursor;
import com.google.common.io.Files;

import java.io.*;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
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
            ostreams[i] = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(out_dir + "/" + String.valueOf(i) + ".bin"), 512 * 1024));


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

            int bucket = Hash.longToBucket(src, bucket_count);
            if (bucket < 0) {
                System.out.printf("%d %d\n", src, bucket);
                Hash.longToBucket(src, bucket_count);
            }
            ostreams[bucket].writeLong(src);
            ostreams[bucket].writeLong(dest);
        }
        for (DataOutputStream s : ostreams)
            s.close();

        for (int i = 0; i < bucket_count; i++)
            ostreams[i] = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(out_dir + "/" + String.valueOf(i) + ".gseg"), 512 * 1024));


        for (int i = 0; i < bucket_count; i++) {
            //DataInputStream istream = new DataInputStream(new BufferedInputStream(new FileInputStream(out_dir + "/" + String.valueOf(i)+".bin"),1024*1024));
            System.out.println(out_dir + "/" + String.valueOf(i) + ".bin");
            //byte[] bb = Files.toByteArray(new File(out_dir + "/" + String.valueOf(i) + ".bin"));
            //DataInputStream istream = new DataInputStream(new ByteArrayInputStream(bb));
            LongBuffer buf = Files.map(new File(out_dir + "/" + String.valueOf(i) + ".bin"), FileChannel.MapMode.READ_ONLY).asLongBuffer();

            Graph g = new Graph();
            while (buf.hasRemaining()) {
//                try {
                long src = buf.get();//istream.readLong();
                long dest = buf.get();//istream.readLong();
                g.vertex_set.add(src);
                    if (!g.adjSet.containsKey(src))
                        g.adjSet.put(src, new LongOpenHashSet());
                    g.adjSet.get(src).add(dest);
//                    if (buf.re == 0)
//                        break;
//                } catch (EOFException exp) {
//                    exp.printStackTrace();
//                    System.exit(-1);
//                }
            }
            //istream.close();
            g.update();
            for (LongObjectCursor<long[]> cur : g.adjArray) {
                ostreams[i].writeLong(cur.key);
                for (long l : cur.value)
                    ostreams[i].writeLong(l);
            }
            ostreams[i].close();
            new File(out_dir + "/" + String.valueOf(i) + ".bin").delete();
        }

    }

}
