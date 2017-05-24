package edu.stanford.futuredata.macrobase.analysis.classify;

import java.lang.Math;
import java.util.concurrent.*;

public class Scorer implements Callable<double[]> {

    private double[] metrics;
    private final double lowCutoff;
    private final double highCutoff;
    private final boolean includeHigh = true;
    private final boolean includeLow = true;

    public Scorer(double[] metrics, double lowCutoff, double highCutoff) {
        this.metrics = metrics;
        this.lowCutoff = lowCutoff;
        this.highCutoff = highCutoff;
    }

    @Override
    public double[] call() throws Exception {
    	try {
	        for (int r = 0; r < metrics.length; r++) {
	            boolean isOutlier = (metrics[r] > highCutoff && includeHigh) || (metrics[r] < lowCutoff && includeLow);
	            metrics[r] = isOutlier ? 1.0 : 0.0;
	        }
	        return metrics;
	    } catch(Exception e) {
	    	System.out.println("Exception");
	    	return metrics;
	    }
    }

}
