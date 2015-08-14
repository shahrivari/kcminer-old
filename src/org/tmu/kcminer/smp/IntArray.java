package org.tmu.kcminer.smp;

/**
 * Created by Saeed on 8/14/2015.
 */
public class IntArray {
    int[] _array;
    int _size = 0;

    public IntArray() {
        _array = new int[4];
        _size = 0;
    }

    public IntArray(int cap) {
        _array = new int[cap];
        _size = 0;
    }


//    public IntArray(int[] array){
//        _array=array;
//        _size=array.length;
//    }
//
//    public IntArray(IntArray src){
//        _array=src._array.clone();
//        _size=src._size;
//    }

    public void fillWith(IntArray src) {
        ensureSize(src._size);
        System.arraycopy(src._array, 0, _array, 0,
                src._size);
        _size = src._size;
    }

    public void fillWithAndOmmitSmallerEquals(int[] src, int w) {
        ensureSize(src.length);
        clear();
        for (int x : src)
            if (x > w)
                add(x);
    }


    public int[] array() {
        return _array;
    }

    public int size() {
        return _size;
    }

    public int capacity() {
        return _array.length;
    }

    public int remaining() {
        return _array.length - _size;
    }

    public void ensureSize(int n) {
        if (n > _array.length) {
            int[] copy = new int[n];
            System.arraycopy(_array, 0, copy, 0,
                    Math.min(_array.length, n));
            _array = copy;
        }
    }

    public void clear() {
        _size = 0;
    }

    public void add(int x) {
        if (_size == _array.length)
            ensureSize(_array.length + 4);
        _array[_size] = x;
        _size++;
    }

    public void intersect(IntArray arr) {
        int i = 0, j = 0, k = 0;
        while (i < _size && j < arr._size) {
            if (_array[i] < arr._array[j])
                i++;
            else if (_array[i] > arr._array[j])
                j++;
            else {
                _array[k++] = _array[i];
                i++;
                j++;
            }
        }
        _size = k;
    }

    public int pop() {
        _size--;
        return _size;
    }

    @Override
    public String toString() {
        if (_array == null)
            return "null";
        int iMax = _size - 1;
        if (iMax == -1)
            return "[]";

        StringBuilder b = new StringBuilder();
        b.append('[');
        for (int i = 0; ; i++) {
            b.append(_array[i]);
            if (i == iMax)
                return b.append(']').toString();
            b.append(", ");
        }
    }

}
