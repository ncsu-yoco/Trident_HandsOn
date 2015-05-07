package storm.starter.trident.project.filters;

import storm.starter.trident.project.countmin.state.BloomFilter;
import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

// checks if the given word is a stop word by comparing it with stopwords stored in BloomFilter
public class FilterStopWords extends BaseFilter {

    private static BloomFilter bloom;

    public FilterStopWords(BloomFilter bloom) {
        this.bloom = bloom;
    }

    // if word is present in Bloom filter implies that the word is a
    //      stopword. it is made unavailbel for further processing
    @Override
    public boolean isKeep(TridentTuple tuple) {
        String word = tuple.getString(0);

        if (bloom.isPresent(word)) {
            //System.err.println("FSWPos:" + word);
            return false;
        }
        //System.err.println("FSWNeg:" + word);
        return true;
    }

}
