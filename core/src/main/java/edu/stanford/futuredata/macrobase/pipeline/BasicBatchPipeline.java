package edu.stanford.futuredata.macrobase.pipeline;

import edu.stanford.futuredata.macrobase.analysis.classify.Classifier;
import edu.stanford.futuredata.macrobase.analysis.classify.PercentileClassifier;
import edu.stanford.futuredata.macrobase.analysis.classify.PredicateClassifier;
import edu.stanford.futuredata.macrobase.analysis.summary.Explanation;
import edu.stanford.futuredata.macrobase.analysis.summary.apriori.APrioriSummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.BatchSummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.fpg.FPGrowthSummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.ratios.ExplanationMetric;
import edu.stanford.futuredata.macrobase.analysis.summary.ratios.GlobalRatioMetric;
import edu.stanford.futuredata.macrobase.analysis.summary.ratios.RiskRatioMetric;
import edu.stanford.futuredata.macrobase.analysis.summary.ratios.PrevalenceRatioMetric;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.util.MacrobaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Simplest default pipeline: load, classify, and then explain
 * Only supports operating over a single metric
 */
public class BasicBatchPipeline implements Pipeline {
    Logger log = LoggerFactory.getLogger(Pipeline.class);

    private final PipelineConfig conf;

    // PipelineConfig params applicable to all classifiers
    private final String metric;
    private final String inputURI;

    // PipelineConfig params applicable to all summarizers
    private final List<String> attributes;
    private final double minSupport;
    private final double minRiskRatio;

    public BasicBatchPipeline (PipelineConfig conf) {
        this.conf = conf;
        inputURI = conf.get("inputURI");

        //classifierType = conf.get("classifier", "percentile");
        metric = conf.get("metric");
        //cutoff = conf.get("cutoff", 1.0);
        //pctileHigh = conf.get("includeHi", true);
        //pctileLow = conf.get("includeLo", true);

        //summarizerType = conf.get("summarizer", "apriori");
        attributes = conf.get("attributes");
        //ratioMetric = conf.get("ratioMetric", "globalRatio");
        minRiskRatio = conf.get("minRatioMetric", 3.0);
        minSupport = conf.get("minSupport", 0.01);
    }

    public Classifier getClassifier() throws MacrobaseException {
        final String classifierType = conf.get("classifier", "percentile");
        switch (classifierType.toLowerCase()) {
            case "percentile": {
                // default values for PercentileClassifier:
                // {cuttoff: 1.0, includeHi: true, includeLo: true}
                final double cutoff = conf.get("cutoff", 1.0);
                final boolean pctileHigh = conf.get("includeHi", true);
                final boolean pctileLow = conf.get("includeLo", true);

                return new PercentileClassifier(metric)
                        .setPercentile(cutoff)
                        .setIncludeHigh(pctileHigh)
                        .setIncludeLow(pctileLow);
            }
            case "predicate": {
                // default values for PredicateClassifier:
                // {predicate: "==", value: 1.0}
                final String predicateStr = conf.get("predicate", "==").trim();
                final double metricValue = conf.get("value", 1.0);
                return new PredicateClassifier(metric, predicateStr, metricValue);
            }
            default : {
                throw new MacrobaseException("Bad Classifier Type");
            }
        }
    }

    public ExplanationMetric getRatioMetric() throws MacrobaseException {
        final String ratioMetric = conf.get("ratioMetric", "globalratio");
        switch (ratioMetric.toLowerCase()) {
            case "globalratio": {
                return new GlobalRatioMetric();
            }
            case "riskratio": {
                return new RiskRatioMetric();
            }
            case "prevalenceratio": {
                return new PrevalenceRatioMetric();
            }
            default: {
                throw new MacrobaseException("Bad Ratio Metric");
            }
        }
    }

    public BatchSummarizer getSummarizer(String outlierColumnName) throws MacrobaseException {
        final String summarizerType = conf.get("summarizer", "apriori");
        final int maxOrder = conf.get("maxOrder", 3);
        if (maxOrder <= 0 || maxOrder > 3) {
            throw new MacrobaseException("0 < maxOrder <= 3 must hold, maxOrder set to " + maxOrder);
        }
        switch (summarizerType.toLowerCase()) {
            case "apriori": {
                APrioriSummarizer summarizer = new APrioriSummarizer();
                summarizer.setOutlierColumn(outlierColumnName)
                        .setAttributes(attributes)
                        .setMinSupport(minSupport)
                        .setMinRatioMetric(minRiskRatio)
                        .setMaxOrder(maxOrder);
                // specific to APriori
                summarizer.setRatioMetric(getRatioMetric());
                return summarizer;
            }
            case "fpgrowth": {
                FPGrowthSummarizer summarizer = new FPGrowthSummarizer();
                summarizer.setOutlierColumn(outlierColumnName)
                        .setAttributes(attributes)
                        .setMinSupport(minSupport)
                        .setMinRatioMetric(minRiskRatio)
                        .setMaxOrder(maxOrder);
                summarizer.setUseAttributeCombinations(true);
                return summarizer;
            }
            default: {
                throw new MacrobaseException("Bad Summarizer Type");
            }
        }
    }

    public DataFrame loadData() throws Exception {
        Map<String, Schema.ColType> colTypes = new HashMap<>();
        colTypes.put(metric, Schema.ColType.DOUBLE);
        return PipelineUtils.loadDataFrame(inputURI, colTypes);
    }

    @Override
    public Explanation results() throws Exception {
        long startTime = System.currentTimeMillis();
        DataFrame df = loadData();
        long elapsed = System.currentTimeMillis() - startTime;

        log.info("Loading time: {}", elapsed);
        log.info("{} rows", df.getNumRows());
        log.info("Metric: {}", metric);
        log.info("Attributes: {}", attributes);

        Classifier classifier = getClassifier();
        classifier.process(df);
        df = classifier.getResults();

        BatchSummarizer summarizer = getSummarizer(classifier.getOutputColumnName());

        startTime = System.currentTimeMillis();
        summarizer.process(df);
        elapsed = System.currentTimeMillis() - startTime;
        log.info("Summarization time: {}", elapsed);
        Explanation output = summarizer.getResults();

        return output;
    }
}
