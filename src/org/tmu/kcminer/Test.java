package org.tmu.kcminer;

import java.io.IOException;

/**
 * Created by Saeed on 8/22/14.
 */
public class Test {
    public static void main(String[] args) throws IOException {
        Stopwatch stopwatch = new Stopwatch().start();
        Graph.layEdgeListToDisk("F:\\nets\\web-BerkStan.txt", "x:\\lay", 32);
        System.out.println(stopwatch);

    }

}
