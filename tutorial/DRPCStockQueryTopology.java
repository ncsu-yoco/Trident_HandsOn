/*
* @author : Kapil Somani
*/
package storm.starter.trident.tutorial;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.state.StateFactory;
import storm.trident.testing.MemoryMapState;
import storm.trident.testing.Split;
// to handle exceptions
import java.io.IOException;
// necessary imports to support spouts and other functions
import storm.trident.operation.builtin.Debug;
import storm.starter.trident.tutorial.spouts.CSVBatchSpout;
import storm.starter.trident.tutorial.functions.SplitFunction;

public class DRPCStockQueryTopology {
    // path of data file relative to pom.xml
    private static final String DATA_PATH = "data/stocks.csv.gz";
    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        conf.setDebug(true);
        conf.setMaxSpoutPending(20);
        // creating new cluster and submitting it
        LocalCluster cluster = new LocalCluster();
        LocalDRPC drpc = new LocalDRPC();
        cluster.submitTopology("stock_drpc", conf, buildTopology(drpc));
        for (int i = 0; i < 10; i++) {
            // creating an event handler to signal begining of operation
            System.out.println("DRPC OOUTPUT : " + drpc.execute("trade_events", "AAPL INTC GE") );
            // queried every 5 secs
            Thread.sleep(5000);
        }
        // Success
        System.out.println("DRPC PROCESS COMPLETE : OK");
        // closing clusters
        cluster.shutdown();
        drpc.shutdown();
    }
    public static StormTopology buildTopology(LocalDRPC drpc) throws IOException {
        // creating new Trident topology
        TridentTopology topology = new TridentTopology();
        // speciying spout and naming the fields it is going to read
        CSVBatchSpout spoutCSV = new CSVBatchSpout(DATA_PATH, new Fields("date", "symbol", "price", "shares"));
        // for persistance
        TridentState tradeVolumeDBMS =
            topology
                .newStream("spout", spoutCSV)	// declare which stream to read from
                //.each(new Fields("date", "symbol", "price", "shares"), new Debug())	// to print tuples
                .groupBy(new Fields("symbol"))	// groupBy to bring all stock entries with given symbol together
                .persistentAggregate(new MemoryMapState.Factory(), new Fields("shares"), new Sum(), new Fields("volume"));	// storing in persistant DB
        topology
            .newDRPCStream("trade_events", drpc)	// on event handler
            .each(new Fields("args"), new SplitFunction(" "), new Fields("symbols"))	// check parameters passed as input
            .stateQuery(tradeVolumeDBMS, new Fields("symbols"), new MapGet(), new Fields("per_symbol_count"))	// quering the persistant DB
            .each(new Fields("symbols", "per_symbol_count"), new FilterNull())	// removing entries woth no values
            .groupBy(new Fields("symbols"))	// grouping by symbols
            .aggregate(new Fields("per_symbol_count"), new Sum(), new Fields("sum"))	// Summing it up
            .project(new Fields("symbols", "sum"));	// projecting the output to print
        return topology.build();
    }
}
