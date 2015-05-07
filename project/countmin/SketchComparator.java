package storm.starter.trident.project.countmin;

import java.util.Comparator;
import storm.starter.trident.project.countmin.state.CountMinSketchState;

public class SketchComparator implements Comparator<String> {

    CountMinSketchState sketch;

    public SketchComparator(CountMinSketchState sketch) {
        this.sketch = sketch;
    }

    public int compare(String o1, String o2) {
        return (int) (sketch.estimateCount(o1) - sketch.estimateCount(o2));
    }

}
