package storm.starter.trident.project.countmin;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import storm.starter.trident.project.countmin.state.CountMinSketchState;
//import java.util.*;
//import java.io.*;

//maintains priority queue to keep track of top-K items in the stream.
// adds new items and queue and returns the list
public class TopList {

    CountMinSketchState sketch;
    PriorityQueue<String> heap;
    int k = Integer.MAX_VALUE;

    // contructor to init values
    public TopList(int k, CountMinSketchState sketch) {
        this.sketch = sketch;
        this.k = k;
        this.heap = new PriorityQueue<String>(k + 1, new SketchComparator(sketch));
    }

    // add elements in the priority queue
    // at any instance priority queue will have atmost k itmes
    public void add(String element) {
        //	sketch.increment(element);
        heap.remove(element);
        heap.add(element);
        while (heap.size() > k) {
            heap.remove();
        }
    }

    // returns the items stored in priority queue
    public List<String> printTopKItems() {
        List<String> items = new ArrayList<String>();
        Iterator<String> i = heap.iterator();
        while (i.hasNext()) {
            items.add(i.next());
        }
        return items;
    }
}
