package org.bptree.AdaptiveSampling;

import java.util.HashMap;
import java.util.Map;

public class DistributionTestResult {
    public String distributionName;
    public double dStatistic;
    public double pValue;
    public double samplingRate;
    public int sampleSize;
    public Map<String, Double> metrics;

    public DistributionTestResult(String name, double d, double p, double rate, int size) {
        this.distributionName = name;
        this.dStatistic = d;
        this.pValue = p;
        this.samplingRate = rate;
        this.sampleSize = size;
        this.metrics = new HashMap<>();
    }
}