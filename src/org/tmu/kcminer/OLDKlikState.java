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
public class OLDKlikState {
    long[] subgraph;
    LongArrayList extension;

    public OLDKlikState(long v, long[] neighbors) {
        subgraph = new long[]{v};
        extension = omitSmallerOrEqualElements(neighbors, v);
    }

    public OLDKlikState() {
    }

    public OLDKlikState expand(long w, long[] w_neighbors) {
        OLDKlikState state = new OLDKlikState();
        state.subgraph = Arrays.copyOf(subgraph, subgraph.length + 1);
        state.subgraph[subgraph.length] = w;
        LongArrayList candids = omitSmallerOrEqualElements(w_neighbors, w);
        state.extension = intersect(extension, candids);
        return state;
    }

    public static long count(Graph g, int k) throws IOException {
        long count = 0;
        Stack<OLDKlikState> q = new Stack<OLDKlikState>();
        for (long v : g.vertices) {
            q.add(new OLDKlikState(v, g.getNeighbors(v)));
        }

        while (!q.isEmpty()) {
            OLDKlikState state = q.pop();
            if (state.subgraph.length == k - 1) {
                count += state.extension.elementsCount;
                continue;
            }
            for (LongCursor w : state.extension) {
                OLDKlikState new_state = state.expand(w.value, g.getNeighbors(w.value));
                if (new_state.subgraph.length + new_state.extension.elementsCount >= k)
                    q.add(new_state);
            }

        }
        System.out.printf("cliques of size %d:  %,d\n", k, count);
        return count;
    }


    public static long parallelCount(final Graph g, final int lower, final int k, final int thread_count) throws IOException, InterruptedException {
        final AtomicLong counter = new AtomicLong();
        final ConcurrentLinkedQueue<Long> cq = new ConcurrentLinkedQueue<Long>();
        for (long v : g.vertices)
            cq.add(v);

        Thread[] threads = new Thread[thread_count];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    int[] clique;
                    while (!cq.isEmpty()) {
                        Long v = cq.poll();
                        if (v == null)
                            break;
                        Stack<OLDKlikState> stack = new Stack<OLDKlikState>();
                        stack.add(new OLDKlikState(v, g.getNeighbors(v)));
                        while (!stack.isEmpty()) {
                            OLDKlikState state = stack.pop();
                            if (state.subgraph.length == k - 1) {
                                counter.getAndAdd(state.extension.elementsCount);
                                continue;
                            }
                            if (state.subgraph.length >= lower - 1)
                                counter.addAndGet(state.extension.elementsCount);
                            for (LongCursor w : state.extension) {
                                OLDKlikState new_state = state.expand(w.value, g.getNeighbors(w.value));
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


}
