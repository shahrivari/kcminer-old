package org.tmu.kcminer.smp;

import java.io.IOException;
import java.util.Stack;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Saeed on 8/14/2015.
 */
public class KlikStateNG2 {
    int[] subgraph;
    int[] extension;
    int extSize = 0;


    private KlikStateNG2() {
    }


    public KlikStateNG2(int v, int[] neighbors) {
        subgraph = new int[]{v};
        extension = new int[neighbors.length];
        for (int i = 0; i < neighbors.length; i++)
            if (neighbors[i] > v)
                extension[extSize++] = neighbors[i];
    }

    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append('[');
        if (subgraph.length == 0)
            b.append(']');
        else
            for (int i = 0; ; i++) {
                b.append(subgraph[i]);
                if (i == subgraph.length - 1) {
                    b.append(']').toString();
                    break;
                }
                b.append(", ");
            }
        b.append("->");
        b.append('[');
        if (extSize == 0)
            b.append(']');
        else
            for (int i = 0; ; i++) {
                b.append(extension[i]);
                if (i == extSize - 1) {
                    b.append(']').toString();
                    break;
                }
                b.append(", ");
            }

        return b.toString();
    }

    public KlikStateNG2 expand(int w, int[] w_neighbors) {
        KlikStateNG2 newState = new KlikStateNG2();
        newState.subgraph = new int[subgraph.length + 1];
        System.arraycopy(subgraph, 0, newState.subgraph, 0, subgraph.length);
        newState.subgraph[subgraph.length] = w;
        newState.extension = new int[extSize];
//        int i = 0, j = 0;
//        while (i < extSize && j < w_neighbors.length) {
//            if (extension[i] < w_neighbors[j])
//                i++;
//            else if (extension[i] > w_neighbors[j])
//                j++;
//            else {
//                if (extension[i] > w)
//                    newState.extension[newState.extSize++] = extension[i];
//                i++;
//                j++;
//            }
//        }
        makeNewExt(newState, w, w_neighbors);
        return newState;
    }

    private void makeNewExt(KlikStateNG2 newState, int w, int[] w_neighbors) {
        //newState.extension = new int[extSize];
        int i = 0, j = 0;
        while (i < extSize && j < w_neighbors.length) {
            if (extension[i] < w_neighbors[j])
                i++;
            else if (extension[i] > w_neighbors[j])
                j++;
            else {
                if (extension[i] > w)
                    newState.extension[newState.extSize++] = extension[i];
                i++;
                j++;
            }
        }
    }

    public static long count(IntGraph g, int l, int k) {
        long count = 0;
        Stack<KlikStateNG2> q = new Stack<KlikStateNG2>();
        for (int v : g.vertices)
            q.add(new KlikStateNG2(v, g.getNeighbors(v)));

        while (!q.isEmpty()) {
            KlikStateNG2 state = q.pop();
            if (state.subgraph.length == k - 1)
                count += state.extSize;

            if (state.subgraph.length >= l)
                count++;
            if (state.subgraph.length == k - 1)
                continue;

            int w = 0;
            for (int i = 0; i < state.extSize; i++) {
                w = state.extension[i];
                KlikStateNG2 new_state = state.expand(w, g.getNeighbors(w));
                if (new_state.subgraph.length + new_state.extSize >= l)
                    q.add(new_state);
            }
        }
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
                    while (!cq.isEmpty()) {
                        Integer v = cq.poll();
                        if (v == null)
                            break;
                        Stack<KlikStateNG2> stack = new Stack<KlikStateNG2>();
                        stack.add(new KlikStateNG2(v, g.getNeighbors(v)));
                        while (!stack.isEmpty()) {
                            KlikStateNG2 state = stack.pop();
                            if (state.subgraph.length == k - 1)
                                counter.getAndAdd(state.extSize);
                            if (state.subgraph.length >= lower)
                                counter.getAndIncrement();
                            if (state.subgraph.length == k - 1)
                                continue;

                            int w = 0;
                            for (int i = 0; i < state.extSize; i++) {
                                w = state.extension[i];
                                KlikStateNG2 new_state = state.expand(w, g.getNeighbors(w));
                                if (new_state.subgraph.length + new_state.extSize >= lower)
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


}
