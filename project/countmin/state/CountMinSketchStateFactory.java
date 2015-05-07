package storm.starter.trident.project.countmin.state;

import storm.trident.state.StateFactory;
import storm.trident.state.State;
import java.util.Map;
import backtype.storm.task.IMetricsContext;

/**
 * @author: Preetham MS (pmahish@ncsu.edu) Modified By: Kapil Somani
 * (kmsomani@ncsu.edu)
 */
public class CountMinSketchStateFactory implements StateFactory {

    protected int depth;
    protected int width;
    protected int seed;
    protected int k;

    // accept parameter K to initialize priority queue
    public CountMinSketchStateFactory(int depth, int width, int seed, int k) {
        this.depth = depth;
        this.width = width;
        this.seed = seed;
        this.k = k;
    }

    // create a new countmin state with given depth and widht
    // also, creates a new priority queue of size k
    @Override
    public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
        return new CountMinSketchState(depth, width, seed, k);
    }
}
