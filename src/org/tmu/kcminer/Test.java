package org.tmu.kcminer;

import org.apache.hadoop.io.Text;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Created by Saeed on 8/22/14.
 */
public class Test {
    public static void main(String[] args) throws IOException, InterruptedException {
        Text txt = new Text("123");
        int l = txt.getLength();
        ByteArrayOutputStream ss = new ByteArrayOutputStream();
        DataOutputStream stream = new DataOutputStream(ss);
        stream.writeChars("123");
        txt.write(stream);
        stream.close();
        ss.close();
        Stopwatch stopwatch = new Stopwatch().start();
        Graph g = Graph.buildFromEdgeListFile("X:\\networks\\wikivote.txt");
        System.out.println(stopwatch);
        stopwatch.reset().start();
        System.out.println(KlikState.parallelEnumerate(g, 3, 4, 8, false, "X:\\a.txt"));

        byte[] arr = g.toBytes();
        Graph gg = Graph.fromBytes(arr);

        System.out.println(KlikState.parallelEnumerate(gg, 3, 4, 8, false, "X:\\a.txt"));

        System.out.println(stopwatch);
        //DiskKlik.enumerate("x:\\lay", 3);
        //System.out.println(stopwatch);

    }

}
