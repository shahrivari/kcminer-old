package org.tmu.kcminer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Created by Saeed on 8/22/14.
 */
public class Util {
    public final int hash32(long key) {
        key = (~key) + (key << 18); // key = (key << 18) - key - 1;
        key ^= (key >>> 31);
        key *= 21; // key = (key + (key << 2)) + (key << 4);
        key ^= (key >>> 11);
        key += (key << 6);
        key ^= (key >>> 22);
        return (int) key;
    }

    public static final int longToBucket(long key, int buckets) {
        key = (~key) + (key << 18); // key = (key << 18) - key - 1;
        key ^= (key >>> 31);
        key *= 21; // key = (key + (key << 2)) + (key << 4);
        key ^= (key >>> 11);
        key += (key << 6);
        key ^= (key >>> 22);
        int result = ((int) key) % buckets;
        return result < 0 ? result + buckets : result;
    }

    public static void deleteDirectory(File f) throws IOException {
        if (f.isDirectory()) {
            for (File c : f.listFiles())
                deleteDirectory(c);
        }
        if (!f.delete())
            throw new FileNotFoundException("Failed to delete file: " + f);
    }


}
