package org.tmu.kcminer;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.cursors.LongCursor;

import java.io.IOException;
import java.util.Arrays;
import java.util.Stack;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Saeed on 8/8/14.
 */
public class NGKlikState {
    long[] subgraph;
    LongArrayList extension;
    LongArrayList tabu;

    public NGKlikState(long v, long[] neighbors) {
        subgraph = new long[]{v};
        extension = LongArrayList.newInstanceWithCapacity(neighbors.length);
        tabu = LongArrayList.newInstanceWithCapacity(neighbors.length);
        for (long n : neighbors)
            if (n <= v)
                tabu.buffer[tabu.elementsCount++] = n;
            else
                extension.buffer[extension.elementsCount++] = n;
    }

    public NGKlikState() {
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

    public NGKlikState expandFixed(long w, long[] w_neighbors) {
        NGKlikState state = new NGKlikState();
        state.subgraph = Arrays.copyOf(subgraph, subgraph.length + 1);
        state.subgraph[subgraph.length] = w;
        LongArrayList candids = omitSmallerOrEqualElements(w_neighbors, w);
        state.extension = intersect(extension, candids);
        return state;
    }

    public NGKlikState expandMax(long w, long[] w_neighbors) {
        NGKlikState state = new NGKlikState();
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
//        Stack<NGKlikState> q = new Stack<NGKlikState>();
//        for (long v : g.vertices) {
//            q.add(new NGKlikState(v, g.getNeighbors(v)));
//        }
//
//        while (!q.isEmpty()) {
//            NGKlikState state = q.pop();
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
//                NGKlikState new_state = state.expandd(w.value, g.getNeighbors(w.value));
//                if (new_state.subgraph.length + new_state.extension.elementsCount >= l)
//                    q.add(new_state);
//            }
//
//        }
//        System.out.printf("cliques of size %d:  %,d\n", k, count);
//        return count;
//    }


    public static long parallelEnumerate(final Graph g, final int lower, final int k, final int thread_count, final boolean maximal) throws IOException, InterruptedException {
        final AtomicLong counter = new AtomicLong();
        final ConcurrentLinkedQueue<Long> cq = new ConcurrentLinkedQueue<Long>();
        for (long v : g.vertices)
            cq.add(v);

        Thread[] threads = new Thread[thread_count];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (!cq.isEmpty()) {
                        Long v = cq.poll();
                        if (v == null)
                            break;
                        Stack<NGKlikState> stack = new Stack<NGKlikState>();
                        stack.add(new NGKlikState(v, g.getNeighbors(v)));
                        while (!stack.isEmpty()) {
                            NGKlikState state = stack.pop();
                            if (state.subgraph.length == k - 1)
                                counter.getAndAdd(state.extension.elementsCount);
                            if (state.subgraph.length >= lower)
                                if (!maximal)
                                    counter.getAndIncrement();
                                else if (state.extension.isEmpty() && state.tabu.isEmpty())
                                    counter.getAndIncrement();
                            if (state.subgraph.length == k - 1)
                                continue;
                            for (LongCursor w : state.extension) {
                                NGKlikState new_state;
                                if (maximal)
                                    new_state = state.expandMax(w.value, g.getNeighbors(w.value));
                                else
                                    new_state = state.expandFixed(w.value, g.getNeighbors(w.value));
                                if (new_state.subgraph.length + new_state.extension.elementsCount >= lower)
                                    stack.add(new_state);
                            }
                        }
                    }

                }
            });
            threads[i].start();
        }

        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
        }
        System.out.printf("cliques of size %d:  %,d\n", k, counter.get());
        return counter.get();
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
}