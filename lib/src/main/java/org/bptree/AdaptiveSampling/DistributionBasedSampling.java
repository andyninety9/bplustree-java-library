package org.bptree.AdaptiveSampling;

import org.bptree.kstest.HandleKSTest;
import org.bptree.sampling.SamplingMethod;

import java.io.IOException;
import java.util.List;

public class DistributionBasedSampling {
    private final SamplingMethod samplingMethod;

    public DistributionBasedSampling(int numPartitions) {
        this.samplingMethod = new SamplingMethod(numPartitions);
    }

    public List<Double> sample(String filePath, int columnIndex) throws IOException {
        // Phát hiện phân phối sử dụng HandleKSTest
        org.bptree.AdaptiveSampling.DistributionTestResult distribution = HandleKSTest.detectDistribution(filePath, columnIndex);

        // Đọc dữ liệu với sampling rate đã được xác định từ HandleKSTest
        List<Double> data = HandleKSTest.readAndPreprocessData(filePath, columnIndex, distribution.samplingRate);

        // Chọn phương thức sampling phù hợp dựa trên phân phối đã phát hiện
        switch (distribution.distributionName) {
            case "Uniform":
                System.out.println("Applying Uniform Sampling...");
                return samplingMethod.uniformSampling(data, distribution.samplingRate);
            case "Gaussian Normal":
                System.out.println("Applying Gaussian Sampling...");
                return samplingMethod.gaussianSampling(data, distribution.samplingRate);
            default:
                System.out.println("Applying Adaptive Sampling for Skewed/Unknown Distribution...");
                return samplingMethod.adaptiveSampling(data, distribution.samplingRate);
        }
    }
}