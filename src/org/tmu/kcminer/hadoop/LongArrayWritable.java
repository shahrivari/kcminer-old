package org.tmu.kcminer.hadoop;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Saeed on 8/25/14.
 */
public class LongArrayWritable implements Writable {
    public long[] array;
    public int termination;

    public LongArrayWritable() {
    }

    public LongArrayWritable(long[] array, int termination) {
        this.array = array;
        this.termination = termination;
    }


    public String toString() {
        if (array == null || array.length == 0)
            return "";
        StringBuilder builder = new StringBuilder();
        for (long l : array)
            builder.append(l).append("\t");
        builder.setLength(builder.length() - 1);
        return builder.toString();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(array.length);
        for (long l : array)
            dataOutput.writeLong(l);
        dataOutput.writeByte(termination);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        int count = dataInput.readInt();
        array = new long[count];
        for (int i = 0; i < count; i++)
            array[i] = dataInput.readLong();
        termination = dataInput.readByte();
    }
}
