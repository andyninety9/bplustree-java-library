package org.bptree.kstest;

import org.apache.commons.math3.distribution.*;
import org.apache.commons.math3.stat.descriptive.moment.*;
import org.apache.commons.math3.stat.inference.KolmogorovSmirnovTest;
import org.bptree.AdaptiveSampling.DistributionTestResult;
import org.bptree.sampling.SamplingMethod;
import org.bptree.utils.FileUtils;

import java.io.*;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class HandleKSTest {
    private static final double ALPHA = 0.05;
    private static final double[] SAMPLING_RATES = {0.05, 0.10, 1.0};
    private static final int MIN_SAMPLE_SIZE = 2000;
    private static final double SKEWNESS_THRESHOLD = 0.5;
    private static final double KURTOSIS_THRESHOLD = 1.0;
    private static final double KURTOSIS_THRESHOLD_LOWER = -4.0;
    private static final double KURTOSIS_THRESHOLD_UPPER = 4.0;

    public static void main(String[] args) {
        String filePath = "lib/src/test/resources/dataset/single_column_normal_distribution1mb.csv";
        int columnIndex = 0;
        SamplingMethod sampler = new SamplingMethod(5);
        try {
            File file = new File(filePath);
            System.out.println("File check:");
            System.out.println("Absolute path: " + file.getAbsolutePath());
            System.out.println("File exists: " + file.exists());
            System.out.println("File size: " + file.length() + " bytes");

            verifyFileContent(filePath);

            DistributionTestResult result = detectDistribution(filePath, columnIndex);
            printResults(result);
            switch(result.distributionName){
                case "Uniform":
                    System.out.println("Applying Uniform Sampling...");
                    List<Double> data = readAndPreprocessData(filePath, columnIndex, 1.0);
                    List<Double> partitions = sampler.uniformSampling(data, 1.0);
                    for (int i = 0; i < partitions.size(); i++) {
                        System.out.println("Partition " + i + ": " + String.format("%.0f", partitions.get(i)));
                    }
                    break;
                case "Gaussian Normal":
                    System.out.println("Applying Gaussian Sampling...");
                    List<Double> data1 = readAndPreprocessData(filePath, columnIndex, 1.0);
                    List<Double> partitions1 = sampler.gaussianSampling(data1, 1.0);
                    for (int i = 0; i < partitions1.size(); i++) {
                        System.out.println("Partition " + i + ": " + String.format("%.0f", partitions1.get(i)));
                    }
                    break;
                default:{
                    System.out.println("Applying Adaptive Sampling for Skewed/Unknown Distribution...");
                    List<Double> data2 = readAndPreprocessData(filePath, columnIndex, 1.0);
                    List<Double> partitions2 = sampler.adaptiveSampling(data2, 1.0);
                    for (int i = 0; i < partitions2.size(); i++) {
                        System.out.println("Partition " + i + ": " + String.format("%.0f", partitions2.get(i)));
                    }
                    break;
                }
            }

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void verifyFileContent(String filePath) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            System.out.println("\nKiểm tra 5 dòng đầu tiên:");
            for (int i = 0; i < 5; i++) {
                String line = br.readLine();
                if (line != null) {
                    System.out.println("Dòng " + (i + 1) + ": " + line);
                }
            }
            System.out.println();
        }
    }

    private static void printResults(DistributionTestResult result) {
        System.out.println("\nKết quả Phân tích Phân phối:");
        System.out.println("Phân phối phát hiện: " + result.distributionName);
        System.out.println("Kích thước mẫu: " + result.sampleSize);
        System.out.println("Tỷ lệ mẫu: " + String.format("%.1f%%", result.samplingRate * 100));
        System.out.println("D-Statistic: " + String.format("%.6f", result.dStatistic));
        System.out.println("P-Value: " + String.format("%.6f", result.pValue));

        if (!result.metrics.isEmpty()) {
            System.out.println("\nChỉ số thống kê bổ sung:");
            result.metrics.forEach((key, value) ->
                    System.out.println(key + ": " + String.format("%.6f", value)));
        }
    }

    public static DistributionTestResult detectDistribution(String filePath, int columnIndex)
            throws IOException {
        for (double samplingRate : SAMPLING_RATES) {
            System.out.println("\nTesting with " + String.format("%.1f%%", samplingRate * 100) + " of data...");

            List<Double> sample = readAndPreprocessData(filePath, columnIndex, samplingRate);
            System.out.println("Number of samples read: " + sample.size());

            if (sample.size() < MIN_SAMPLE_SIZE) {
                if (samplingRate == SAMPLING_RATES[SAMPLING_RATES.length - 1]) {
                    // If we're at the highest sampling rate and still don't have enough samples,
                    // proceed with what we have
                    System.out.println("Proceeding with available samples despite small size.");
                } else {
                    System.out.println("Sample size too small, increasing sampling rate...");
                    continue;
                }
            }

            double[] stats = calculateBasicStatistics(sample);
            System.out.println("Basic statistics:");
            System.out.println("Mean: " + stats[0]);
            System.out.println("StdDev: " + stats[1]);
            System.out.println("Skewness: " + stats[2]);
            System.out.println("Kurtosis: " + stats[3]);

            return testDistributions(sample, samplingRate, stats);
        }

        // If we've exhausted all sampling rates and still can't get enough samples,
        // default to Skew Gaussian (or you could choose another default based on your needs)
        return new DistributionTestResult("Skew Gaussian", 1.0, 0.0,
                SAMPLING_RATES[SAMPLING_RATES.length - 1], 0);
    }

    public static List<Double> readAndPreprocessData(String filePath, int columnIndex, double sampleRate)
            throws IOException {
        List<String[]> allRows = FileUtils.readCSV(filePath, ",");
        System.out.println("Tổng số dòng trong file: " + allRows.size());

        int targetSampleSize = Math.max((int)(allRows.size() * sampleRate), MIN_SAMPLE_SIZE);

        List<Double> samples = allRows.parallelStream()
                .filter(row -> row != null && row.length > columnIndex)
                .map(row -> {
                    try {
                        return Double.parseDouble(row[columnIndex].trim());
                    } catch (NumberFormatException e) {
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .filter(value -> ThreadLocalRandom.current().nextDouble() < sampleRate)
                .limit(targetSampleSize)
                .collect(Collectors.toList());

        Collections.sort(samples);
        return samples;
    }

    private static DistributionTestResult testDistributions(List<Double> sample, double samplingRate, double[] stats) {
        double mean = stats[0];
        double std = stats[1];
        double skewness = stats[2];
        double kurtosis = stats[3];

        Map<String, double[]> testResults = new HashMap<>();
        KolmogorovSmirnovTest ksTest = new KolmogorovSmirnovTest();
        double[] data = sample.stream().mapToDouble(Double::doubleValue).toArray();

        // Test for Normal distribution
        NormalDistribution normalDist = new NormalDistribution(mean, std);
        double normalD = ksTest.kolmogorovSmirnovStatistic(normalDist, data);
        double normalP = ksTest.kolmogorovSmirnovTest(normalDist, data);
        testResults.put("Gaussian Normal", new double[]{normalD, normalP});

        // Test for Uniform distribution
        double min = Collections.min(sample);
        double max = Collections.max(sample);
        UniformRealDistribution uniformDist = new UniformRealDistribution(min, max);
        double uniformD = ksTest.kolmogorovSmirnovStatistic(uniformDist, data);
        double uniformP = ksTest.kolmogorovSmirnovTest(uniformDist, data);
        testResults.put("Uniform", new double[]{uniformD, uniformP});

        // Check for Skew Normal distribution
        boolean isSkewed = Math.abs(skewness) > SKEWNESS_THRESHOLD;
        boolean isKurtosisAbnormal = Math.abs(kurtosis - 3) > KURTOSIS_THRESHOLD;

        if (isSkewed || isKurtosisAbnormal) {
            // Only consider Skew Normal if it's clearly not Normal or Uniform
            if (normalP < ALPHA && uniformP < ALPHA) {
                testResults.put("Skew Normal", new double[]{0.0, 1.0});
            }
        }

        return selectBestDistribution(testResults, sample, samplingRate, stats);
    }

    private static boolean isLikelyNormal(double skewness, double kurtosis) {
        return Math.abs(skewness) < SKEWNESS_THRESHOLD
                && kurtosis > KURTOSIS_THRESHOLD_LOWER
                && kurtosis < KURTOSIS_THRESHOLD_UPPER;
    }

    private static double[] calculateBasicStatistics(List<Double> data) {
        double[] values = data.stream().mapToDouble(d -> d).toArray();
        return new double[] {
                new Mean().evaluate(values),
                new StandardDeviation().evaluate(values),
                new Skewness().evaluate(values),
                new Kurtosis().evaluate(values)
        };
    }

    private static double[] calculateEmpiricalDistribution(List<Double> data) {
        int n = data.size();
        double[] ecdf = new double[n];
        for (int i = 0; i < n; i++) {
            ecdf[i] = (i + 0.5) / n;
        }
        return ecdf;
    }

    private static double[] calculateTheoreticalNormal(List<Double> data) {
        NormalDistribution normalDist = new NormalDistribution();
        return data.stream()
                .mapToDouble(normalDist::cumulativeProbability)
                .toArray();
    }

    private static double[] calculateTheoreticalUniform(List<Double> data, double min, double max) {
        UniformRealDistribution uniformDist = new UniformRealDistribution(min, max);
        return data.stream()
                .mapToDouble(x -> uniformDist.cumulativeProbability(x))
                .toArray();
    }

    private static double calculateDStatistic(double[] dist1, double[] dist2) {
        return IntStream.range(0, dist1.length)
                .mapToDouble(i -> Math.abs(dist1[i] - dist2[i]))
                .max()
                .orElse(1.0);
    }

    private static double calculateModifiedPValue(double dStatistic, int sampleSize) {
        double n = sampleSize;
        double lambda = (Math.sqrt(n) + 0.12 + 0.11/Math.sqrt(n)) * dStatistic;

        double sum = 0;
        for (int i = 1; i <= 3; i++) {
            sum += Math.exp(-2 * i * i * lambda * lambda);
        }
        return 2 * sum;
    }

    private static DistributionTestResult selectBestDistribution(
            Map<String, double[]> testResults,
            List<Double> sample,
            double samplingRate,
            double[] stats) {

        Map.Entry<String, double[]> bestFit = testResults.entrySet().stream()
                .max(Comparator.comparingDouble(e -> e.getValue()[1]))
                .orElse(null);

        String distributionName;
        double dStatistic;
        double pValue;

        if (bestFit == null || bestFit.getValue()[1] < ALPHA) {
            // Nếu không có phân phối nào phù hợp hoặc p-value thấp hơn ALPHA,
            // chúng ta sẽ chọn Skew Gaussian
            distributionName = "Skew Gaussian";
            dStatistic = 1.0;
            pValue = 0.0;
        } else {
            distributionName = bestFit.getKey();
            dStatistic = bestFit.getValue()[0];
            pValue = bestFit.getValue()[1];
        }

        DistributionTestResult result = new DistributionTestResult(
                distributionName,
                dStatistic,
                pValue,
                samplingRate,
                sample.size()
        );

        result.metrics.put("Mean", stats[0]);
        result.metrics.put("StandardDeviation", stats[1]);
        result.metrics.put("Skewness", stats[2]);
        result.metrics.put("Kurtosis", stats[3]);
        result.metrics.put("CoefficientOfVariation", stats[1]/stats[0]);

        return result;
    }


}