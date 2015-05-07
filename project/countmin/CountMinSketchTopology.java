package storm.starter.trident.project.countmin;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.StormSubmitter;
import backtype.storm.tuple.Fields;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.state.StateFactory;
import storm.trident.testing.MemoryMapState;
import storm.trident.testing.Split;
import storm.trident.testing.FixedBatchSpout;
import backtype.storm.tuple.Values;
import backtype.storm.tuple.Fields;

import storm.trident.operation.builtin.Count;

import storm.starter.trident.project.countmin.state.CountMinSketchStateFactory;
import storm.starter.trident.project.countmin.state.CountMinQuery;
import storm.starter.trident.project.countmin.state.CountMinSketchUpdater;
import storm.starter.trident.tutorial.functions.SplitFunction;

import storm.starter.trident.project.spouts.TwitterSampleSpout;
import storm.starter.trident.project.functions.*;
import storm.starter.trident.project.filters.*;

import storm.trident.testing.MemoryMapState;
import storm.trident.state.StateFactory;

import storm.starter.trident.project.countmin.state.CountMinSketchState;

// necessary imports to support functionality
import storm.starter.trident.project.countmin.state.BloomFilter;
import storm.starter.trident.project.countmin.state.CountMinTopK;

import java.util.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

//import storm.starter.trident.project.countmin.TopList;
/**
 * @author: Preetham MS (pmahish@ncsu.edu) edited by: Kapil Somani
 * (kmsomani@ncsu.edu)
 */
public class CountMinSketchTopology {

    // create a new BloomFilter
    static BloomFilter bf = new BloomFilter(1000, 5);

    //number of top items to be displayed is determined by value of k.
    // the value should be passed as command line parameter
    static int k = Integer.MAX_VALUE;

    public static StormTopology buildTopology(String[] args, LocalDRPC drpc) throws IOException {

        // creating new topology
        TridentTopology topology = new TridentTopology();

        int width = 10;
        int depth = 15;
        int seed = 10;

        // API keys for twitter are passed as command line parameteres
        String consumerKey = args[0];
        String consumerSecret = args[1];
        String accessToken = args[2];
        String accessTokenSecret = args[3];

        // 5th argument in command line specified value of K in top-K items
        k = Integer.parseInt(args[4]);

        //String[] arguments = args.clone();
//	String[] arguments = {"CK", "CS", "AT", "AS", "love", "hate", "Alfred"};
        String[] topicWords = Arrays.copyOfRange(args, 4, args.length);

        // creating spout to fetch tweets
        TwitterSampleSpout spoutTweets = new TwitterSampleSpout(consumerKey, consumerSecret,
                accessToken, accessTokenSecret, topicWords);
        /*
         FixedBatchSpout spoutFixedBatch = new FixedBatchSpout(new Fields("sentence"), 3,
         new Values("the cow jumped over the moon"),
         new Values("the man went to the store and bought some candy"),
         new Values("four score and seven years ago"),
         new Values("how many apples can you eat"),
         new Values("to be or not to be the person"))
         ;
         spoutFixedBatch.setCycle(false);
         */

        TridentState countMinDBMS = topology
                .newStream("tweets", spoutTweets) //declare which stream to read from
                .each(new Fields("tweet"), new ParseTweet(), new Fields("text", "tweetId", "user")) //parse tweets into known fields
                //.each(new Fields("text","tweetId","user"), new PrintFilter("PARSED TWEETS:"))
                .each(new Fields("text", "tweetId", "user"), new SentenceBuilder(), new Fields("sentence"))
                .each(new Fields("sentence"), new Split(), new Fields("words")) //splits the text into individual words
                .each(new Fields("words"), new FilterStopWords(bf)) // remove stop-words by comparing it with the words stored in BloomFilter
                //.each(new Fields("words"), new Print("FilteredKeywords:"))
                .partitionPersist(new CountMinSketchStateFactory(depth, width, seed, k), new Fields("words"), new CountMinSketchUpdater()); // update countMin sketch by incrementing values of words which appear in the tweets

        topology.newDRPCStream("get_count", drpc) // on even 'get_count'
                .each(new Fields("args"), new Split(), new Fields("query")) // split passed arguments(not relevant in this iplementation)
                //.stateQuery(countMinDBMS, new Fields("query"), new CountMinQuery(), new Fields("count"))
                .stateQuery(countMinDBMS, new CountMinTopK(), new Fields("count")) // query the countMin sketch to get top-K items
                .project(new Fields("count"));  //project to print top items in the stream

        return topology.build();

    }

    public static void main(String[] args) throws Exception {

        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxSpoutPending(10);

        // Read file containing list of stop words and add all
        //      stopwords into BloomFilter
        BufferedReader br = new BufferedReader(new FileReader("data/stopwords.txt"));
        try {
            String word = br.readLine();
            while (word != null) {
                //System.out.println("StopWord: '" + word + "'");
                // adding word into BloomFilter
                bf.add(word);
                word = br.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            br.close();
        }

        // create a new topolgy and submit it
        LocalCluster cluster = new LocalCluster();
        LocalDRPC drpc = new LocalDRPC();
        cluster.submitTopology("get_count", conf, buildTopology(args, drpc));

        for (int i = 0; i < 5; i++) {
            // executing topolgy with 'get_count' event
            System.out.println("DRPC RESULT:" + drpc.execute("get_count", "a"));
            // queried every 5 second
            Thread.sleep(5000);
        }

        System.out.println("STATUS: OK");

        //closing cluster
        cluster.shutdown();
        drpc.shutdown();
    }
}
