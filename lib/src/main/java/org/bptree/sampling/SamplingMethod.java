package org.bptree.sampling;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * A class to perform Reservoir Sampling.
 */
public class SamplingMethod {

    private int numPartitions;

    /**
     * Constructor to initialize SamplingMethod.
     *
     * @param numPartitions Number of partitions desired.
     */
    public SamplingMethod(int numPartitions) {
        this.numPartitions = numPartitions;
    }

    /**
     * Perform Reservoir Sampling to generate boundary partitions.
     *
     * @param data The dataset to sample from.
     * @return A sorted list of boundary partitions.
     */
    public List<Double> reservoirSampling(List<Double> data) {
        List<Double> reservoir = new ArrayList<>();
        Random random = new Random();

        // Fill the reservoir with the first numPartitions elements
        for (int i = 0; i < data.size(); i++) {
            if (i < numPartitions) {
                reservoir.add(data.get(i));
            } else {
                // Replace elements in the reservoir with decreasing probability
                int j = random.nextInt(i + 1);
                if (j < numPartitions) {
                    reservoir.set(j, data.get(i));
                }
            }
        }

        // Sort the reservoir to create boundary partitions
        reservoir.sort(Double::compareTo);
        return reservoir;
    }

    /**
     * Perform Quantile-Based Sampling to generate boundary partitions.
     *
     * @param data The dataset to sample from.
     * @return A sorted list of boundary partitions.
     */
    public List<Double> quantileBasedSampling(List<Double> data) {
        List<Double> boundaries = new ArrayList<>();

        // Sort the data to calculate quantiles
        List<Double> sortedData = new ArrayList<>(data);
        Collections.sort(sortedData);

        // Calculate the quantile positions
        for (int i = 1; i < numPartitions; i++) {
            double quantilePosition = (double) i / numPartitions * (sortedData.size() - 1);
            int lowerIndex = (int) Math.floor(quantilePosition);
            int upperIndex = (int) Math.ceil(quantilePosition);

            // Interpolate if necessary
            double quantileValue;
            if (lowerIndex == upperIndex) {
                quantileValue = sortedData.get(lowerIndex);
            } else {
                double lowerValue = sortedData.get(lowerIndex);
                double upperValue = sortedData.get(upperIndex);
                quantileValue = lowerValue + (quantilePosition - lowerIndex) * (upperValue - lowerValue);
            }

            boundaries.add(quantileValue);
        }

        return boundaries;
    }

    /**
     * Perform Stratified Sampling for Exponential Distribution.
     *
     * @param data       The dataset to sample from.
     * @param lambdaRate The rate parameter (lambda) of the Exponential Distribution.
     * @return A sorted list of boundary partitions.
     */
    public List<Double> stratifiedSamplingExponential(List<Double> data, double lambdaRate) {
        // Sort the dataset to ensure stratification works effectively
        List<Double> sortedData = new ArrayList<>(data);
        Collections.sort(sortedData);

        // Calculate the cumulative distribution (CDF) boundaries for each partition
        List<Double> boundaries = new ArrayList<>();
        int totalSize = sortedData.size();
        for (int i = 1; i < numPartitions; i++) {
            // Determine the quantile position
            double quantile = (double) i / numPartitions;

            // Find the corresponding value in the sorted dataset using the CDF of Exponential Distribution
            double boundary = -Math.log(1 - quantile) / lambdaRate;

            // If the boundary is beyond the range of the dataset, use the max value
            boundary = Math.min(boundary, sortedData.get(totalSize - 1));

            boundaries.add(boundary);
        }

        return boundaries;
    }

    /**
     * Perform Quantile-Based Sampling for LogNormal Distribution.
     *
     * @param data   The dataset to sample from.
     * @param mu     The mean of the log-transformed data.
     * @param sigma  The standard deviation of the log-transformed data.
     * @return A sorted list of boundary partitions.
     */
    public List<Double> quantileBasedSamplingLogNormal(List<Double> data, double mu, double sigma) {
        // Sort the dataset to ensure quantiles can be calculated effectively
        List<Double> sortedData = new ArrayList<>(data);
        Collections.sort(sortedData);

        // Calculate boundary partitions using quantiles
        List<Double> boundaries = new ArrayList<>();
        int totalSize = sortedData.size();
        for (int i = 1; i < numPartitions; i++) {
            // Determine the quantile position
            double quantile = (double) i / numPartitions;

            // Calculate the boundary using the inverse CDF of the LogNormal Distribution
            double boundary = Math.exp(mu + sigma * Math.sqrt(2) * inverseErf(2 * quantile - 1));

            // If the boundary exceeds the dataset range, use the max value
            boundary = Math.min(boundary, sortedData.get(totalSize - 1));

            boundaries.add(boundary);
        }

        return boundaries;
    }

    /**
     * Approximation of the inverse error function (erf^-1).
     *
     * @param x Input value (-1 < x < 1).
     * @return Approximation of the inverse error function.
     */
    private double inverseErf(double x) {
        // Constants for approximation
        double a = 0.147; // Abramowitz and Stegun approximation constant
        double ln = Math.log(1 - x * x);
        double term1 = (2 / (Math.PI * a)) + (ln / 2);
        double term2 = ln / a;
        return Math.signum(x) * Math.sqrt(Math.sqrt(term1 * term1 - term2) - term1);
    }
}
