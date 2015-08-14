package org.tmu.kcminer.smp;

import com.google.common.base.Stopwatch;

import java.io.IOException;

/**
 * Created by Saeed on 8/14/2015.
 */
public class TestNG {
    public static void main(String[] args) throws IOException, InterruptedException {
        IntGraph graph = new IntGraph();
        Stopwatch stopwatch = Stopwatch.createStarted();
        //graph.buildFromEdgeListFile("X:\\networks\\kcminer\\web-NotreDame.txt");
        graph.buildFromEdgeListFile(args[0]);
        int size = Integer.parseInt(args[1]);
        int threads = Integer.parseInt(args[2]);
        System.out.println(stopwatch.toString());
        stopwatch.reset().start();

        long count3 = IntKlikState.parallelCount(graph, size, size, threads);
        System.out.println(count3 + "  " + stopwatch.toString());
        stopwatch.reset().start();


        long count = KlikStateNG2.parallelCount(graph, size, size, threads);
        System.out.println(count + "  " + stopwatch.toString());
        stopwatch.reset().start();


        return;
    }
}
