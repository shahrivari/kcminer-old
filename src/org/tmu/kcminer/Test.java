package org.tmu.kcminer;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by Saeed on 8/22/14.
 */
public class Test {
    public static void main(String[] args) throws IOException {
        int x = Util.longToBucket(241, 90);
        int segments = 16;
        Stopwatch stopwatch = new Stopwatch().start();
        Graph g = Graph.buildFromEdgeListFile("X:\\networks\\slash.txt");

        int buckets = 10;
        FileWriter[] writers = new FileWriter[buckets];
        String path = "z:\\gg";
        HashMap<Integer, Set<Long>> table = new HashMap<Integer, Set<Long>>();
        for (int i = 0; i < buckets; i++) {
            table.put(i, new HashSet<Long>());
            writers[i] = new FileWriter(path + i);
        }

        for (long l : g.vertices) {
            table.get(Util.longToBucket(l, buckets)).add(l);
            String arr = Arrays.toString(g.getNeighbors(l));
            for (long m : g.getNeighbors(l))
                //table.get(Util.longToBucket(m,buckets)).add(l);
                writers[Util.longToBucket(m, buckets)].write(arr + "\n");
        }

//        for(Map.Entry<Integer,Set<Long>> x:table.entrySet()){
//            long sum=0;
//            for(long l:x.getValue())
//                sum+=g.getNeighbors(l).length;
//            System.out.printf("%,d\n",sum);
//        }

        //Graph.layEdgeListToDisk("X:\\networks\\twitter_combined.txt", "x:\\lay", segments);
        for (FileWriter w : writers)
            w.close();
        System.out.println(stopwatch);
        stopwatch.reset().start();
        //DiskKlik.enumerate("x:\\lay", 3);
        System.out.println(stopwatch);

    }

}
