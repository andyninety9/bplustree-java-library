package org.bptree.sampling;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;

/**
 * A class to perform different sampling methods based on data distributions
 */
public class SamplingMethod {
    private int numPartitions;

    public SamplingMethod(int numPartitions) {
        this.numPartitions = numPartitions;
    }

    /**
     * Uniform distribution sampling using systematic approach
     */
    public List<Double> uniformSampling(List<Double> data, double sampleRatio) {
        int n = data.size();
        int sampleSize = (int)(n * sampleRatio);
        int step = n / sampleSize;

        // Systematic sampling
        List<Double> samples = new ArrayList<>();
        for (int i = 0; i < n; i += step) {
            if (samples.size() < sampleSize) {
                samples.add(data.get(i));
            }
        }

        // Calculate boundaries
        List<Double> boundaries = new ArrayList<>();
        int partitionSize = samples.size() / numPartitions;
        for (int i = 1; i < numPartitions; i++) {
            boundaries.add(samples.get(i * partitionSize));
        }

        Collections.sort(boundaries);
        return boundaries;
    }

    /**
     * Gaussian distribution sampling using stratified approach based on normal distribution properties
     */
    public List<Double> gaussianSampling(List<Double> data, double sampleRatio) {
        int n = data.size();
        int sampleSize = (int)(n * sampleRatio);

        // Calculate mean and standard deviation
        double mean = calculateMean(data);
        double std = calculateStd(data, mean);

        // Create samples based on normal distribution properties
        List<Double> samples = new ArrayList<>();

        // Calculate percentiles based on standard normal distribution
        for (int i = 0; i < numPartitions; i++) {
            // Use normal distribution percentiles
            double z = getNormalPercentile((i + 1.0) / numPartitions);
            double value = mean + z * std;

            // Find closest value in data
            double closestValue = data.stream()
                    .min((a, b) -> Double.compare(
                            Math.abs(a - value),
                            Math.abs(b - value)))
                    .orElse(value);

            samples.add(closestValue);
        }

        Collections.sort(samples);

        // Calculate boundaries
        List<Double> boundaries = new ArrayList<>();
        for (int i = 1; i < numPartitions; i++) {
            boundaries.add(samples.get(i));
        }

        return boundaries;
    }

    /**
     * Get percentile point from standard normal distribution using inverse error function
     */
    private double getNormalPercentile(double p) {
        // Approximation of inverse error function
        double a = 0.147;
        double x = 2*p - 1;
        double ln = Math.log(1 - x*x);
        double term1 = (2/(Math.PI*a)) + (ln/2);
        double term2 = ln/a;

        // Convert to z-score
        return Math.sqrt(2) * Math.signum(p - 0.5) *
                Math.sqrt(Math.sqrt(term1*term1 - term2) - term1);
    }

    /**
     * Adaptive sampling for skewed distributions using weighted sampling
     */
    public List<Double> adaptiveSampling(List<Double> data, double sampleRatio) {
        int n = data.size();
        int sampleSize = (int)(n * sampleRatio);

        // Calculate mean and skewness
        double mean = calculateMean(data);
        double std = calculateStd(data, mean);
        double skewness = calculateSkewness(data, mean, std);

        // Calculate weights based on skewness
        double[] weights = new double[n];
        double maxWeight = Double.MIN_VALUE;

        for (int i = 0; i < n; i++) {
            double value = data.get(i);
            double normalizedDist = (value - mean) / std;

            if (skewness > 0) { // Right skewed
                // Give higher weights to larger values
                weights[i] = Math.exp(normalizedDist);
            } else { // Left skewed
                // Give higher weights to smaller values
                weights[i] = Math.exp(-normalizedDist);
            }
            maxWeight = Math.max(maxWeight, weights[i]);
        }

        // Normalize weights to [0,1]
        for (int i = 0; i < n; i++) {
            weights[i] /= maxWeight;
        }

        // Weighted random sampling
        List<Double> samples = weightedSampling(data, weights, sampleSize);
        Collections.sort(samples);

        // Calculate boundaries
        List<Double> boundaries = new ArrayList<>();
        int partitionSize = samples.size() / numPartitions;
        for (int i = 1; i < numPartitions; i++) {
            boundaries.add(samples.get(i * partitionSize));
        }

        return boundaries;
    }

    // Add helper method to calculate standard deviation
    private double calculateStd(List<Double> data, double mean) {
        return Math.sqrt(
                data.stream()
                        .mapToDouble(value -> Math.pow(value - mean, 2))
                        .average()
                        .orElse(0.0)
        );
    }

    // Update skewness calculation to use std
    private double calculateSkewness(List<Double> data, double mean, double std) {
        double n = data.size();
        double sumCubed = data.stream()
                .mapToDouble(value -> Math.pow((value - mean) / std, 3))
                .sum();
        return sumCubed / n;
    }

    private double calculateMean(List<Double> data) {
        return data.stream()
                .mapToDouble(Double::doubleValue)
                .average()
                .orElse(0.0);
    }

    private double calculateSkewness(List<Double> data, double mean) {
        double n = data.size();
        double sumCubed = 0.0;
        double variance = 0.0;

        for (double value : data) {
            double diff = value - mean;
            sumCubed += Math.pow(diff, 3);
            variance += diff * diff;
        }

        variance = variance / n;
        double stdDev = Math.sqrt(variance);
        return (sumCubed / n) / Math.pow(stdDev, 3);
    }

    private List<Double> weightedSampling(List<Double> data, double[] weights, int sampleSize) {
        List<Double> samples = new ArrayList<>();
        Random random = new Random();

        // Reservoir sampling with weights
        TreeMap<Double, Double> reservoir = new TreeMap<>();
        for (int i = 0; i < data.size(); i++) {
            double key = Math.pow(random.nextDouble(), 1.0 / weights[i]);
            if (reservoir.size() < sampleSize) {
                reservoir.put(key, data.get(i));
            } else if (key > reservoir.firstKey()) {
                reservoir.remove(reservoir.firstKey());
                reservoir.put(key, data.get(i));
            }
        }

        samples.addAll(reservoir.values());
        return samples;
    }
    /**
     * Random sampling that selects random points from the dataset.
     */
    /**
     * Random sampling that ensures unique values are selected.
     */
    public List<Double> randomSampling(List<Double> data, double sampleRatio) {
        int n = data.size();
        int sampleSize = (int) (n * sampleRatio);

        // Shuffle the data without a fixed seed
        Collections.shuffle(data, new Random(System.currentTimeMillis()));

        // Ensure unique sampling by selecting the first sampleSize elements
        List<Double> samples = new ArrayList<>(data.subList(0, sampleSize));
        Collections.sort(samples);

        // Calculate boundaries
        List<Double> boundaries = new ArrayList<>();
        int partitionSize = sampleSize / numPartitions;
        for (int i = 1; i < numPartitions; i++) {
            boundaries.add(samples.get(i * partitionSize));
        }

        return boundaries;
    }


}