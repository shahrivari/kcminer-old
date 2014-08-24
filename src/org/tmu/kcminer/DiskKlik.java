package org.tmu.kcminer;

import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;

import java.io.*;

/**
 * Created by Saeed on 8/24/14.
 */
public class DiskKlik {

    public static void enumerate(String root_dir, int k) throws IOException {
        if (!new File(root_dir + "/graph").isDirectory()) {
            System.out.println("Root directory");
        }
        int segments = new File(root_dir + "/graph").list().length;

        //Initial step
        new File(root_dir + "/1/").delete();
        new File(root_dir + "/1/").mkdir();
        DataOutputStream[] ostreams = new DataOutputStream[segments];
        for (int segment = 0; segment < segments; segment++)
            ostreams[segment] = new DataOutputStream(new LZ4BlockOutputStream(new FileOutputStream(root_dir + "/1/" + segment + ".part"), 256 * 1024));

        for (int segment = 0; segment < segments; segment++) {
            Graph g = Graph.loadFromSegment(root_dir, segment);
            for (long v : g.vertices) {
                KlikState state = new KlikState(v, g.getNeighbors(v));
                byte[] bytes = state.toBytes();
                int bucket = Util.longToBucket(v, segments);
                ostreams[bucket].writeInt(bytes.length);
                ostreams[bucket].write(bytes);
            }
        }
        for (DataOutputStream osteam : ostreams)
            osteam.close();

        for (int current_size = 2; current_size <= k; current_size++) {
            for (int segment = 0; segment < segments; segment++) {
                Graph g = Graph.loadFromSegment(root_dir, segment);
                DataInputStream inputStream = new DataInputStream(new LZ4BlockInputStream(new FileInputStream(root_dir + (current_size - 1) + segment + ".part")));
                int count = inputStream.readInt();
                byte[] bytes = new byte[count];
                inputStream.read(bytes);
                KlikState state = KlikState.fromBytes(bytes);
                KlikState new_state = state.expandFixed(state.w, g.getNeighbors(state.w));

            }

        }


    }


}
