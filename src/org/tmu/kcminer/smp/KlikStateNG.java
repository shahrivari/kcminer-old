package org.tmu.kcminer.smp;

import java.util.Stack;

/**
 * Created by Saeed on 8/14/2015.
 */
public class KlikStateNG {
    IntArray subgraph;
    IntArray extension;

    static Stack<KlikStateNG> freeList = new Stack<KlikStateNG>();

    private KlikStateNG() {
        subgraph = new IntArray(4);
        extension = new IntArray(4);
    }

    public static KlikStateNG getNew() {
        if (freeList.size() == 0)
            return new KlikStateNG();
        else {
            KlikStateNG state = freeList.pop();
            state.subgraph.clear();
            state.extension.clear();
            return state;
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

    public KlikStateNG expand(int w, int[] w_neighbors) {
        KlikStateNG state = getNew();
        state.subgraph.fillWith(subgraph);
        state.subgraph.add(w);
        state.extension.fillWithAndOmmitSmallerEquals(w_neighbors, w);
        state.extension.intersect(extension);
        return state;
    }

}
