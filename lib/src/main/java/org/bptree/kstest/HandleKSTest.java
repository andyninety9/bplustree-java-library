package org.bptree.kstest;

import org.apache.commons.math3.distribution.*;
import org.apache.commons.math3.stat.inference.KolmogorovSmirnovTest;
import org.bptree.sampling.SamplingMethod;
import org.bptree.utils.FileUtils;

import java.io.IOException;
import java.util.*;

/**
 * HandleKSTest performs distribution detection and optimal sampling rate selection
 * for datasets. It uses Kolmogorov-Smirnov (KS) Test to determine the best-fit distribution
 * and applies appropriate sampling methods for further data processing.
 */
public class HandleKSTest {
    private static final int NUM_TEST_ITERATIONS = 1; // Number of iterations to stabilize sampling results
    private static final double[] INITIAL_SAMPLING_RATES = {0.01, 0.05, 0.1};
    private static final double MAX_SAMPLING_RATE = 0.5; // Maximum sampling rate to try
    private static final double RATE_INCREMENT = 0.1; // Increment for sampling rate in each iteration

    public static void main(String[] args) {
        String filePath = "lib/src/test/resources/dataset/dataset_100mb.csv"; // Path to dataset
        int columnIndex = 0; // Index of the column to analyze
        int numPartitions = 5; // Number of partitions for sampling

        try {
            // Find the optimal sampling rate to detect a valid distribution
            double optimalRate = findOptimalSamplingRate(filePath, columnIndex);
            System.out.println("Selected optimal sampling rate: " + String.format("%.2f%%", optimalRate * 100));

            // Read the dataset using the optimal sampling rate
            List<Double> data = readNumericColumn(filePath, columnIndex, optimalRate);

            // Detect the distribution of the data
            String detectedDistribution = detectDistribution(data);

            // Choose and apply the appropriate sampling method
            chooseSamplingMethod(detectedDistribution, data, numPartitions);
        } catch (IOException e) {
            System.err.println("Error reading data file: " + e.getMessage());
        }
    }

    /**
     * Finds the optimal sampling rate for detecting a valid distribution.
     * Iteratively tests different sampling rates until a valid distribution is found.
     *
     * @param filePath    The path to the CSV file.
     * @param columnIndex The column index to analyze.
     * @return The optimal sampling rate.
     * @throws IOException If an error occurs while reading the file.
     */
    private static double findOptimalSamplingRate(String filePath, int columnIndex) throws IOException {
        for (double initialRate : INITIAL_SAMPLING_RATES) {
            double currentRate = initialRate;

            while (currentRate <= MAX_SAMPLING_RATE) {
                System.out.println("\nTesting sampling rate: " + String.format("%.2f%%", currentRate * 100));

                boolean validDistributionFound = false;

                // Perform multiple iterations to stabilize the p-value
                for (int i = 0; i < NUM_TEST_ITERATIONS; i++) {
                    System.out.println("Iteration " + (i + 1) + " of " + NUM_TEST_ITERATIONS);

                    // Sample the data using the current rate
                    List<Double> sample = readNumericColumn(filePath, columnIndex, currentRate);

                    // Test distributions on the sampled data
                    Map<String, Double> distributionTests = testAllDistributions(sample);

                    // Check if any distribution has p-value >= 0.05
                    if (distributionTests.values().stream().anyMatch(pValue -> pValue >= 0.05)) {
                        validDistributionFound = true;
                        break;
                    }
                }

                // If a valid distribution is found, return the current rate
                if (validDistributionFound) {
                    return currentRate;
                }

                // Increment the sampling rate for the next iteration
                currentRate += RATE_INCREMENT;
                System.out.println("P-value < 0.05, increasing sampling rate to: " +
                        String.format("%.2f%%", currentRate * 100));
            }
        }

        System.out.println("Warning: No valid distribution found even with maximum sampling rate");
        return MAX_SAMPLING_RATE;
    }

    /**
     * Reads numeric data from a specific column in a CSV file.
     * Applies random sampling based on the specified sample rate.
     *
     * @param filePath    The path to the CSV file.
     * @param columnIndex The column index to extract data from.
     * @param sampleRate  The proportion of rows to sample.
     * @return A list of sampled numeric values.
     * @throws IOException If an error occurs while reading the file.
     */
    public static List<Double> readNumericColumn(String filePath, int columnIndex, double sampleRate) throws IOException {
        List<String[]> rows = FileUtils.readCSV(filePath, ",");
        List<Double> columnData = new ArrayList<>();
        Random random = new Random();
//        Collections.sort(columnData);
        // Iterate through rows and sample data randomly
        for (String[] row : rows) {
            if (row.length > columnIndex && random.nextDouble() < sampleRate) {
                try {
                    columnData.add(Double.parseDouble(row[columnIndex]));
                } catch (NumberFormatException e) {
                    System.err.println("Skipping invalid numeric value: " + row[columnIndex]);
                }
            }
        }

        // Warn if the sample size is too small
        if (columnData.size() < 50) {
            System.out.println("Warning: Sample size may be too small for reliable distribution detection");
        }

        return columnData;
    }

    /**
     * Tests multiple distributions (Normal, Uniform, Exponential, LogNormal)
     * against the data using Kolmogorov-Smirnov Test.
     *
     * @param data The dataset to test.
     * @return A map of distribution names to their p-values.
     */
    private static Map<String, Double> testAllDistributions(List<Double> data) {
//        Collections.sort(data);
        KolmogorovSmirnovTest ksTest = new KolmogorovSmirnovTest();
        Map<String, Double> results = new HashMap<>();

        // Define the distributions to test
        Map<String, AbstractRealDistribution> distributions = new HashMap<>();
        distributions.put("Normal", new NormalDistribution(mean(data), stdDev(data)));
        distributions.put("Uniform", new UniformRealDistribution(Collections.min(data), Collections.max(data)));
        distributions.put("Exponential", new ExponentialDistribution(mean(data)));
        distributions.put("LogNormal", new LogNormalDistribution(mean(data), stdDev(data)));

        // Perform KS Test for each distribution
        for (Map.Entry<String, AbstractRealDistribution> entry : distributions.entrySet()) {
            double[] sample = data.stream().mapToDouble(d -> d).toArray();
            double pValue = ksTest.kolmogorovSmirnovTest(entry.getValue(), sample);
            results.put(entry.getKey(), pValue);
            System.out.println(entry.getKey() + " distribution p-value: " + pValue);
        }

        return results;
    }

    /**
     * Detects the best-fit distribution for the dataset by selecting the one
     *
     * @param data The dataset to analyze.
     * @return The name of the best-fit distribution or "No matching distribution".
     */

    public static String detectDistribution(List<Double> data) {
        Map<String, Double> results = testAllDistributions(data);

        return results.entrySet().stream()
                .filter(entry -> entry.getValue() >= 0.05)
                .max(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .orElse("No matching distribution");
    }

    /**
     * Chooses the appropriate sampling method based on the detected distribution.
     *
     * @param distribution The detected distribution.
     * @param data         The dataset to sample.
     * @param numPartitions The number of partitions desired.
     */
    public static void chooseSamplingMethod(String distribution, List<Double> data, int numPartitions) {
        SamplingMethod sampler = new SamplingMethod(numPartitions);

        switch (distribution) {
            case "Uniform":
                System.out.println("Using quantile-based Sampling for Uniform Distribution...");
                List<Double> quantileBoundaries = sampler.quantileBasedSampling(data);
                System.out.println("Boundary Partitions (Quantile-Based Sampling): " + quantileBoundaries);
                break;

            case "Normal":
                System.out.println("Normal distribution detected. Implement a sampling method here.");
                break;

            case "Exponential":
                System.out.println("Using Stratified Sampling for Exponential Distribution...");
                double lambdaRate = 0.5;
                List<Double> stratifiedBoundaries = sampler.stratifiedSamplingExponential(data, lambdaRate);
                System.out.println("Boundary Partitions (Stratified Sampling): " + stratifiedBoundaries);
                break;

            case "LogNormal":
                System.out.println("Using quantile-based Sampling for LogNormal Distribution...");
                double mu = 1.0;
                double sigma = 0.5;
                List<Double> logNormalBoundaries = sampler.quantileBasedSamplingLogNormal(data, mu, sigma);
                System.out.println("Boundary Partitions (LogNormal Sampling): " + logNormalBoundaries);
                break;

            default:
                throw new IllegalArgumentException("No suitable sampling method for distribution: " + distribution);
        }
    }

    /**
     * Calculates the mean of a list of numeric values.
     *
     * @param data The dataset.
     * @return The mean value.
     */
    public static double mean(List<Double> data) {
        return data.stream().mapToDouble(d -> d).average().orElse(0.0);
    }

    /**
     * Calculates the standard deviation of a list of numeric values.
     *
     * @param data The dataset.
     * @return The standard deviation.
     */
    public static double stdDev(List<Double> data) {
        double mean = mean(data);
        return Math.sqrt(data.stream().mapToDouble(d -> Math.pow(d - mean, 2)).average().orElse(0.0));
    }
}
