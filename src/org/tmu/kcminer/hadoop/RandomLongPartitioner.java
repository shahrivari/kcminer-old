package org.tmu.kcminer.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.tmu.kcminer.Util;


/**
 * Created by Saeed on 8/30/14.
 */
public class RandomLongPartitioner extends Partitioner<LongWritable, Writable> {
    @Override
    public int getPartition(LongWritable lw, Writable writable, int bucks) {
        return Util.longToBucket(lw.get(), bucks);
    }
}
