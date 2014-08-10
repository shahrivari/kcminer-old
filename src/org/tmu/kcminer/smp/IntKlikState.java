package org.tmu.kcminer.smp;

import java.io.IOException;
import java.util.Arrays;
import java.util.Stack;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Saeed on 8/8/14.
 */
public class IntKlikState {
    int[] subgraph;
    int[] extension;

    public IntKlikState(int v, int[] neighbors) {
        subgraph = new int[]{v};
        extension = omitSmallerOrEqualElements(neighbors, v);
    }

    public IntKlikState() {
    }

    public IntKlikState expand(int w, int[] w_neighbors) {
        IntKlikState state = new IntKlikState();
        state.subgraph = Arrays.copyOf(subgraph, subgraph.length + 1);
        state.subgraph[subgraph.length] = w;
        int[] candids = omitSmallerOrEqualElements(w_neighbors, w);
        state.extension = intersect(extension, candids);
        return state;
    }

    public static long count(IntGraph g, int k) throws IOException {
        long count = 0;
        Stack<IntKlikState> q = new Stack<IntKlikState>();
        for (int v : g.vertices) {
            q.add(new IntKlikState(v, g.getNeighbors(v)));
        }

        while (!q.isEmpty()) {
            IntKlikState state = q.pop();
            if (state.subgraph.length == k - 1) {
                count += state.extension.length;
                continue;
            }
            for (int w : state.extension) {
                IntKlikState new_state = state.expand(w, g.getNeighbors(w));
                if (new_state.subgraph.length + new_state.extension.length >= k)
                    q.add(new_state);
            }
        }
        System.out.printf("cliques of size %d:  %,d\n", k, count);
        return count;
    }

    public static long parallelCount(final IntGraph g, final int lower, final int k, final int thread_count) throws IOException, InterruptedException {
        final AtomicLong counter = new AtomicLong();
        final ConcurrentLinkedQueue<Integer> cq = new ConcurrentLinkedQueue<Integer>();
        for (int v : g.vertices)
            cq.add(v);

        Thread[] threads = new Thread[thread_count];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    int[] clique;
                    while (!cq.isEmpty()) {
                        Integer v = cq.poll();
                        if (v == null)
                            break;
                        Stack<IntKlikState> stack = new Stack<IntKlikState>();
                        stack.add(new IntKlikState(v, g.getNeighbors(v)));
                        while (!stack.isEmpty()) {
                            IntKlikState state = stack.pop();
                            if (state.subgraph.length == k - 1) {
                                counter.getAndAdd(state.extension.length);
                                continue;
                            }
                            if (state.subgraph.length >= lower - 1)
                                counter.addAndGet(state.extension.length);
                            for (int w : state.extension) {
                                IntKlikState new_state = state.expand(w, g.getNeighbors(w));
                                if (new_state.subgraph.length + new_state.extension.length >= lower)
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
        return counter.get();
    }


    private static int[] omitSmallerOrEqualElements(int[] list, int x) {
        int[] result = new int[list.length];
        int new_size = 0;
        for (int i = 0; i < list.length; i++)
            if (list[i] > x)
                result[new_size++] = list[i];
        return Arrays.copyOf(result, new_size);
    }

    private static int[] intersect(int[] sorted1, int[] sorted2) {
        int[] result = new int[Math.min(sorted1.length, sorted2.length)];
        int i = 0, j = 0, k = 0;

        while (i < sorted1.length && j < sorted2.length) {
            if (sorted1[i] < sorted2[j])
                i++;
            else if (sorted1[i] > sorted2[j])
                j++;
            else {
                result[k++] = sorted1[i];
                i++;
                j++;
            }
        }
        return Arrays.copyOf(result, k);
    }

}
