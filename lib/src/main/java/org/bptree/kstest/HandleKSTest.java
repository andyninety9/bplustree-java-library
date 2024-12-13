package org.bptree.kstest;

import org.apache.commons.math3.distribution.*;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.apache.commons.math3.stat.descriptive.moment.Skewness;
import org.apache.commons.math3.stat.descriptive.moment.Kurtosis;
import org.bptree.utils.FileUtils;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class HandleKSTest {
    // Các ngưỡng thống kê và tham số lấy mẫu
    private static final double ALPHA = 0.05;                    // Mức ý nghĩa thống kê
    private static final double[] SAMPLING_RATES = {0.05, 0.10, 0.20}; // Tỷ lệ mẫu tăng dần
    private static final int MIN_SAMPLE_SIZE = 1000;            // Kích thước mẫu tối thiểu
    private static final double SKEWNESS_THRESHOLD = 0.5;       // Ngưỡng độ lệch cho Gaussian
    private static final double KURTOSIS_THRESHOLD = 1.0;       // Ngưỡng độ nhọn cho Gaussian

    public static void main(String[] args) {
        String filePath = "lib/src/test/resources/dataset/dataset_100mb.csv";
        int columnIndex = 0;

        try {
            ProgressiveTestResult result = detectDistributionProgressively(filePath, columnIndex);
            printResults(result);
        } catch (IOException e) {
            System.err.println("Error processing data: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static class ProgressiveTestResult {
        String distributionName;
        double dStatistic;
        double pValue;
        double samplingRate;
        int sampleSize;
        Map<String, Double> additionalStats;  // Thêm thống kê bổ sung

        ProgressiveTestResult(String name, double d, double p, double rate, int size) {
            this.distributionName = name;
            this.dStatistic = d;
            this.pValue = p;
            this.samplingRate = rate;
            this.sampleSize = size;
            this.additionalStats = new HashMap<>();
        }
    }

    private static void printResults(ProgressiveTestResult result) {
        System.out.println("\nKết quả Phân tích Phân phối:");
        System.out.println("Phân phối phát hiện: " + result.distributionName);
        System.out.println("Kích thước mẫu: " + result.sampleSize);
        System.out.println("Tỷ lệ mẫu: " + String.format("%.1f%%", result.samplingRate * 100));
        System.out.println("D-Statistic: " + String.format("%.6f", result.dStatistic));
        System.out.println("P-Value: " + String.format("%.6f", result.pValue));

        // In thống kê bổ sung nếu có
        if (!result.additionalStats.isEmpty()) {
            System.out.println("\nThống kê bổ sung:");
            result.additionalStats.forEach((key, value) ->
                    System.out.println(key + ": " + String.format("%.6f", value)));
        }
    }

    public static ProgressiveTestResult detectDistributionProgressively(String filePath, int columnIndex)
            throws IOException {

        for (double samplingRate : SAMPLING_RATES) {
            System.out.println("\nKiểm tra với mẫu " + String.format("%.1f%%", samplingRate * 100) + "...");

            List<Double> sample = readNumericColumnParallel(filePath, columnIndex, samplingRate);

            if (sample.size() < MIN_SAMPLE_SIZE) {
                System.out.println("Kích thước mẫu quá nhỏ, tăng tỷ lệ mẫu...");
                continue;
            }

            Collections.sort(sample);

            // Tính toán thống kê cơ bản và phân phối thực nghiệm
            double[] stats = calculateBasicStatistics(sample);
            double mean = stats[0], std = stats[1];
            double skewness = stats[2], kurtosis = stats[3];

            double[] empiricalDist = calculateEmpiricalDistribution(sample);

            // Kiểm tra đặc tính Gaussian trước
            boolean hasGaussianProperties = checkGaussianProperties(skewness, kurtosis);

            // Test các phân phối
            Map<String, double[]> testResults = new HashMap<>();

            // Ưu tiên kiểm tra Gaussian nếu có đặc tính phù hợp
            if (hasGaussianProperties) {
                double[] gaussianDist = calculateTheoreticalGaussian(sample, mean, std);
                double gaussianD = calculateDStatistic(empiricalDist, gaussianDist);
                double gaussianP = calculatePValue(gaussianD, sample.size());
                testResults.put("Gaussian", new double[]{gaussianD, gaussianP});
            }

            // Kiểm tra phân phối đều
            double[] uniformDist = calculateTheoreticalUniform(sample,
                    Collections.min(sample),
                    Collections.max(sample));
            double uniformD = calculateDStatistic(empiricalDist, uniformDist);
            double uniformP = calculatePValue(uniformD, sample.size());
            testResults.put("Uniform", new double[]{uniformD, uniformP});

            // Tìm phân phối phù hợp nhất
            Map.Entry<String, double[]> bestFit = testResults.entrySet().stream()
                    .min(Comparator.comparingDouble(e -> e.getValue()[0]))
                    .orElse(null);

            if (bestFit != null && bestFit.getValue()[1] >= ALPHA) {
                ProgressiveTestResult result = new ProgressiveTestResult(
                        bestFit.getKey(),
                        bestFit.getValue()[0],
                        bestFit.getValue()[1],
                        samplingRate,
                        sample.size()
                );

                // Thêm thống kê bổ sung
                result.additionalStats.put("Skewness", skewness);
                result.additionalStats.put("Kurtosis", kurtosis);
                result.additionalStats.put("Mean", mean);
                result.additionalStats.put("StandardDeviation", std);

                return result;
            }

            System.out.println("Không tìm thấy phân phối phù hợp ở tỷ lệ mẫu hiện tại");
        }

        return new ProgressiveTestResult("Adaptive", 1.0, 0.0,
                SAMPLING_RATES[SAMPLING_RATES.length - 1], 0);
    }

    private static double[] calculateBasicStatistics(List<Double> data) {
        double[] arrayData = data.stream().mapToDouble(d -> d).toArray();
        Mean mean = new Mean();
        StandardDeviation std = new StandardDeviation();
        Skewness skewness = new Skewness();
        Kurtosis kurtosis = new Kurtosis();

        return new double[] {
                mean.evaluate(arrayData),
                std.evaluate(arrayData),
                skewness.evaluate(arrayData),
                kurtosis.evaluate(arrayData)
        };
    }

    private static boolean checkGaussianProperties(double skewness, double kurtosis) {
        return Math.abs(skewness) < SKEWNESS_THRESHOLD &&
                Math.abs(kurtosis - 3) < KURTOSIS_THRESHOLD;
    }

    private static double[] calculateEmpiricalDistribution(List<Double> data) {
        int n = data.size();
        double[] ecdf = new double[n];
        // Sử dụng phương pháp điểm giữa để xử lý tốt hơn tính liên tục
        for (int i = 0; i < n; i++) {
            ecdf[i] = (i + 0.5) / n;
        }
        return ecdf;
    }

    private static double[] calculateTheoreticalGaussian(List<Double> data, double mean, double std) {
        NormalDistribution normalDist = new NormalDistribution(mean, std);
        return data.stream()
                .mapToDouble(x -> normalDist.cumulativeProbability(x))
                .toArray();
    }

    private static double[] calculateTheoreticalUniform(List<Double> data, double min, double max) {
        UniformRealDistribution uniformDist = new UniformRealDistribution(min, max);
        return data.stream()
                .mapToDouble(x -> uniformDist.cumulativeProbability(x))
                .toArray();
    }

    private static double calculateDStatistic(double[] dist1, double[] dist2) {
        if (dist1.length != dist2.length) {
            throw new IllegalArgumentException("Distributions must have the same length");
        }
        return IntStream.range(0, dist1.length)
                .mapToDouble(i -> Math.abs(dist1[i] - dist2[i]))
                .max()
                .orElse(1.0);
    }

    private static double calculatePValue(double dStatistic, int sampleSize) {
        // Cải thiện tính toán p-value cho độ chính xác cao hơn
        double n = sampleSize;
        double lambda = (Math.sqrt(n) + 0.12 + 0.11/Math.sqrt(n)) * dStatistic;

        double sum = 0;
        for (int i = 1; i <= 3; i++) {
            sum += Math.exp(-2 * i * i * lambda * lambda);
        }
        return 2 * sum;
    }

    public static List<Double> readNumericColumnParallel(String filePath, int columnIndex, double sampleRate)
            throws IOException {
        List<String[]> rows = FileUtils.readCSV(filePath, ",");
        return rows.parallelStream()
                .filter(row -> row.length > columnIndex && Math.random() < sampleRate)
                .map(row -> {
                    try {
                        return Double.parseDouble(row[columnIndex]);
                    } catch (NumberFormatException e) {
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }
}