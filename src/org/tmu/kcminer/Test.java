package org.tmu.kcminer;

import java.io.IOException;

/**
 * Created by Saeed on 8/22/14.
 */
public class Test {
    public static void main(String[] args) throws IOException {
        int segments = 16;
        KlikState state = new KlikState(123, new long[]{1, 2, 3, 4, 5});
        byte[] bb = state.toBytes();
        KlikState state1 = KlikState.fromBytes(bb);
        Stopwatch stopwatch = new Stopwatch().start();
        Graph.layEdgeListToDisk("X:\\networks\\twitter_combined.txt", "x:\\lay", segments);
        System.out.println(stopwatch);
        stopwatch.reset().start();
        DiskKlik.enumerate("x:\\lay", 3);
        System.out.println(stopwatch);

    }

}
