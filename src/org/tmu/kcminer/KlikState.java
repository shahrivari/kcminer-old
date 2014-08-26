package org.tmu.kcminer;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.cursors.LongCursor;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Stack;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by Saeed on 8/8/14.
 */
public class KlikState {
    public long[] subgraph;
    public LongArrayList extension;
    public LongArrayList tabu;

    static private FileWriter writer = null;
    final static private ReentrantLock lock = new ReentrantLock();
    final static int flushLimit = 1024 * 1024;


    public KlikState(long v, long[] neighbors) {
        subgraph = new long[]{v};
        extension = LongArrayList.newInstanceWithCapacity(neighbors.length);
        tabu = LongArrayList.newInstanceWithCapacity(neighbors.length);
        for (long n : neighbors)
            if (n <= v)
                tabu.buffer[tabu.elementsCount++] = n;
            else
                extension.buffer[extension.elementsCount++] = n;
    }

    private KlikState() {
    }

    public long[] toLongs() {
        int tabu_size = tabu != null ? tabu.size() : 0;
        long[] array = new long[subgraph.length + extension.size() + tabu_size + 3];
        int index = 0;

        array[index++] = subgraph.length;
        for (long x : subgraph)
            array[index++] = x;

        array[index++] = extension.size();
        for (LongCursor x : extension)
            array[index++] = x.value;

        array[index++] = tabu_size;
        if (tabu_size > 0)
            for (LongCursor x : tabu)
                array[index++] = x.value;
        return array;
    }

    public static KlikState fromLongs(long[] array) {
        int index = 0;
        KlikState state = new KlikState();
        int count = (int) array[index++];
        state.subgraph = Arrays.copyOfRange(array, index, index + count);
        index += count;

        count = (int) array[index++];
        state.extension = new LongArrayList(count);
        state.extension.buffer = Arrays.copyOfRange(array, index, index + count);
        state.extension.elementsCount = count;
        index += count;

        count = (int) array[index++];
        state.tabu = new LongArrayList(count);
        state.tabu.buffer = Arrays.copyOfRange(array, index, index + count);
        state.tabu.elementsCount = count;

        return state;
    }

    public byte[] toBytes() {
        ByteBuffer bb = ByteBuffer.allocate(3 * Integer.SIZE + subgraph.length * Long.SIZE + extension.elementsCount * Long.SIZE + tabu.elementsCount * Long.SIZE + Long.SIZE);
        bb.putInt(subgraph.length);
        for (long x : subgraph)
            bb.putLong(x);
        bb.putInt(extension.elementsCount);
        for (LongCursor x : extension)
            bb.putLong(x.value);
        if (tabu != null) {
            bb.putInt(tabu.elementsCount);
            for (LongCursor x : tabu)
                bb.putLong(x.value);
        } else
            bb.putInt(0);
        return bb.array();
    }

    public static KlikState fromBytes(byte[] bytes) {
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        KlikState state = new KlikState();
        int count;
        count = bb.getInt();
        state.subgraph = new long[count];
        for (int i = 0; i < count; i++)
            state.subgraph[i] = bb.getLong();

        count = bb.getInt();
        state.extension = new LongArrayList(count);
        for (int i = 0; i < count; i++)
            state.extension.buffer[i] = bb.getLong();
        state.extension.elementsCount = count;

        count = bb.getInt();
        state.tabu = new LongArrayList(count);
        for (int i = 0; i < count; i++)
            state.tabu.buffer[i] = bb.getLong();
        state.tabu.elementsCount = count;


        return state;
    }

    public String toString() {
        String result = "";
        if (subgraph != null)
            result += "sub:" + Arrays.toString(subgraph);
        if (extension != null)
            result += "ext:" + extension.toString();
        if (tabu != null)
            result += "tab:" + tabu.toString();
        return result;
    }

    public long[] getClique(long w) {
        long[] clique = Arrays.copyOf(subgraph, subgraph.length + 1);
        clique[clique.length - 1] = w;
        return clique;
    }

    public KlikState expandFixed(long w, long[] w_neighbors) {
        KlikState state = new KlikState();
        state.subgraph = Arrays.copyOf(subgraph, subgraph.length + 1);
        state.subgraph[subgraph.length] = w;
        LongArrayList candids = omitSmallerOrEqualElements(w_neighbors, w);
        state.extension = intersect(extension, candids);
        return state;
    }

    public KlikState expandMax(long w, long[] w_neighbors) {
        KlikState state = new KlikState();
        state.subgraph = Arrays.copyOf(subgraph, subgraph.length + 1);
        state.subgraph[subgraph.length] = w;
        state.tabu = intersect(tabu, w_neighbors);
        LongArrayList potential = intersect(extension, w_neighbors);
        LongArrayList smalls = new LongArrayList(potential.size());
        LongArrayList bigs = new LongArrayList(potential.size());
        for (int i = 0; i < potential.elementsCount; i++)
            if (potential.buffer[i] <= w)
                smalls.buffer[smalls.elementsCount++] = potential.buffer[i];
            else
                bigs.buffer[bigs.elementsCount++] = potential.buffer[i];
        state.extension = intersect(extension, bigs);
        state.tabu = union(state.tabu, smalls);
        return state;
    }

//    public static long count(Graph g, int l, int k) throws IOException {
//        long count = 0;
//        Stack<KlikState> q = new Stack<KlikState>();
//        for (long v : g.vertices) {
//            q.add(new KlikState(v, g.getNeighbors(v)));
//        }
//
//        while (!q.isEmpty()) {
//            KlikState state = q.pop();
//            if(state.subgraph.length>=l&&state.tabu.isEmpty()&&state.extension.isEmpty()){
//                //System.out.println(Arrays.toString(state.subgraph));
//            }
//            if (state.subgraph.length == k - 1) {
//                long[] found=Arrays.copyOf(state.subgraph,k);
//                for(int i=0;i<state.extension.elementsCount;i++){
//                    found[k-1]= state.extension.buffer[i];
//                    //System.out.println(Arrays.toString(found));
//                }
//                count += state.extension.elementsCount;
//                continue;
//            }
//            for (LongCursor w : state.extension) {
//                KlikState new_state = state.expandd(w.value, g.getNeighbors(w.value));
//                if (new_state.subgraph.length + new_state.extension.elementsCount >= l)
//                    q.add(new_state);
//            }
//
//        }
//        System.out.printf("cliques of size %d:  %,d\n", k, count);
//        return count;
//    }


    public static long parallelEnumerate(final Graph g, final int lower, final int k, final int thread_count, final boolean maximal, final String path) throws IOException, InterruptedException {
        final AtomicLong counter = new AtomicLong();
        final ConcurrentLinkedQueue<Long> cq = new ConcurrentLinkedQueue<Long>();
        for (long v : g.vertices)
            cq.add(v);

        if (path != null)
            writer = new FileWriter(path);
        final FileWriter writer = KlikState.writer;


        Thread[] threads = new Thread[thread_count];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    StringBuilder builder = new StringBuilder(10240);
                    while (!cq.isEmpty()) {
                        Long v = cq.poll();
                        if (v == null)
                            break;
                        Stack<KlikState> stack = new Stack<KlikState>();
                        stack.add(new KlikState(v, g.getNeighbors(v)));
                        while (!stack.isEmpty()) {
                            KlikState state = stack.pop();
                            if (state.subgraph.length == k - 1) {
                                counter.getAndAdd(state.extension.elementsCount);
                                if (writer != null)
                                    for (LongCursor cursor : state.extension)
                                        builder.append(cliqueToString(state.getClique(cursor.value))).append("\n");
                            }
                            if (state.subgraph.length >= lower)
                                if (!maximal) {
                                    counter.getAndIncrement();
                                    if (writer != null)
                                        builder.append(cliqueToString(state.subgraph)).append("\n");
                                } else if (state.extension.isEmpty() && state.tabu.isEmpty()) {
                                    counter.getAndIncrement();
                                    if (writer != null)
                                        builder.append(cliqueToString(state.subgraph)).append("\n");
                                }
                            if (state.subgraph.length == k - 1)
                                continue;
                            for (LongCursor w : state.extension) {
                                KlikState new_state;
                                if (maximal)
                                    new_state = state.expandMax(w.value, g.getNeighbors(w.value));
                                else
                                    new_state = state.expandFixed(w.value, g.getNeighbors(w.value));
                                if (new_state.subgraph.length + new_state.extension.elementsCount >= lower)
                                    stack.add(new_state);
                            }
                            if (builder.length() > flushLimit && writer != null)
                                flush(builder);
                        }
                    }
                    if (writer != null)
                        flush(builder);
                }
            });
            threads[i].start();
        }

        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
        }
        if (writer != null)
            writer.close();
        return counter.get();
    }

    private static void flush(StringBuilder builder) {
        lock.lock();
        try {
            writer.write(builder.toString());
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        } finally {
            lock.unlock();
        }
        builder.setLength(0);
    }


    private LongArrayList omitSmallerOrEqualElements(long[] list, long x) {
        LongArrayList result = new LongArrayList(list.length);
        for (int i = 0; i < list.length; i++)
            if (list[i] > x)
                result.buffer[result.elementsCount++] = list[i];
        return result;
    }

    private static LongArrayList intersect(LongArrayList sorted1, LongArrayList sorted2) {
        LongArrayList result = new LongArrayList(Math.min(sorted1.elementsCount, sorted2.elementsCount));
        int i = 0, j = 0, k = 0;
        while (i < sorted1.elementsCount && j < sorted2.elementsCount) {
            if (sorted1.buffer[i] < sorted2.buffer[j])
                i++;
            else if (sorted1.buffer[i] > sorted2.buffer[j])
                j++;
            else {
                result.buffer[result.elementsCount++] = sorted1.buffer[i];
                i++;
                j++;
            }
        }
        return result;
    }

    private static LongArrayList intersect(LongArrayList sorted1, long[] sorted2) {
        LongArrayList result = new LongArrayList(Math.min(sorted1.elementsCount, sorted2.length));
        int i = 0, j = 0, k = 0;
        while (i < sorted1.elementsCount && j < sorted2.length) {
            if (sorted1.buffer[i] < sorted2[j])
                i++;
            else if (sorted1.buffer[i] > sorted2[j])
                j++;
            else {
                result.buffer[result.elementsCount++] = sorted1.buffer[i];
                i++;
                j++;
            }
        }
        return result;
    }

    public static LongArrayList union(LongArrayList sorted1, LongArrayList sorted2) {
        LongArrayList result = new LongArrayList(sorted1.elementsCount + sorted2.elementsCount);
        int i = 0, j = 0, k = 0;

        while (i < sorted1.elementsCount && j < sorted2.elementsCount) {
            if (sorted1.buffer[i] < sorted2.buffer[j])
                result.buffer[k] = sorted1.buffer[i++];
            else if (sorted1.buffer[i] > sorted2.buffer[j])
                result.buffer[k] = sorted2.buffer[j++];
            else { //equal
                result.buffer[k] = sorted2.buffer[j];
                j++;
                i++;
            }
            k++;
        }
        while (i < sorted1.elementsCount)
            result.buffer[k++] = sorted1.buffer[i++];

        while (j < sorted2.elementsCount)
            result.buffer[k++] = sorted2.buffer[j++];

        result.elementsCount = k;
        return result;
    }

    public static String cliqueToString(long[] clique) {
        StringBuilder builder = new StringBuilder(clique.length * 10);
        if (clique.length == 1)
            return Long.toString(clique[0]);
        for (int i = 0; i < clique.length - 1; i++)
            builder.append(clique[i]).append("\t");
        builder.append(clique[clique.length - 1]);
        return builder.toString();
    }
}