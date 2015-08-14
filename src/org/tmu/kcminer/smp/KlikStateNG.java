package org.tmu.kcminer.smp;

import java.util.Stack;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Saeed on 8/14/2015.
 */
public class KlikStateNG {
    IntArray subgraph;
    IntArray extension;

    static Stack<KlikStateNG> freeList = new Stack<KlikStateNG>();
    static AtomicLong newRequest = new AtomicLong(0);
    static AtomicLong allocations = new AtomicLong(0);


    private KlikStateNG() {
        subgraph = new IntArray(4);
        extension = new IntArray(4);
    }

    public static KlikStateNG getNew() {
        newRequest.incrementAndGet();
        if (freeList.size() == 0) {
            allocations.incrementAndGet();
            return new KlikStateNG();
        }
        else {
            KlikStateNG state;
            synchronized (freeList) {
                state = freeList.pop();
            }
            state.subgraph.clear();
            state.extension.clear();
            return state;
        }
    }

    public static void release(KlikStateNG state) {
        if (freeList.size() > 1000)
            return;
        else
            synchronized (freeList) {
                freeList.add(state);
            }
    }

    public KlikStateNG(int v, int[] neighbors) {
        subgraph = new IntArray(4);
        subgraph.add(v);
        extension = new IntArray(neighbors.length);
        for (int i = 0; i < neighbors.length; i++)
            if (neighbors[i] > v)
                extension.add(neighbors[i]);
    }

    public String toString() {
        return subgraph.toString() + "->" + extension.toString();
    }

    public KlikStateNG expand(int w, int[] w_neighbors) {
        KlikStateNG state = new KlikStateNG();
        state.subgraph.fillWith(subgraph);
        state.subgraph.add(w);
        state.extension.fillWithAndOmmitSmallerEquals(w_neighbors, w);
        state.extension.intersect(extension);
        return state;
    }

    public static long count(IntGraph g, int l, int k) {
        long count = 0;
        Stack<KlikStateNG> q = new Stack<KlikStateNG>();
        for (int v : g.vertices)
            q.add(new KlikStateNG(v, g.getNeighbors(v)));

        while (!q.isEmpty()) {
            KlikStateNG state = q.pop();
            if (state.subgraph.size() == k - 1)
                count += state.extension.size();

            if (state.subgraph.size() >= l)
                count++;
            if (state.subgraph.size() == k - 1)
                continue;

            int w = 0;
            for (int i = 0; i < state.extension.size(); i++) {
                w = state.extension._array[i];
                KlikStateNG new_state = state.expand(w, g.getNeighbors(w));
                if (new_state.subgraph.size() + new_state.extension.size() >= l)
                    q.add(new_state);
            }
            //release(state);
        }
        return count;
    }


}
