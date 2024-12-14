package org.bptree.sampling;

import net.minidev.json.JSONObject;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import java.util.ArrayList;
import java.util.List;

public class SparkSampling {
//
//    public static void main(String[] args) throws Exception {
//        // Ensure that we have the required arguments
//        if (args.length < 3) {
//            System.err.println("Usage: SparkSampling <input-path> <output-path> <num-partitions>");
//            System.exit(1);
//        }
//
//        // Parse input arguments
//        String inputPath = args[0];
//        String outputPath = args[1];
//        int numPartitions = Integer.parseInt(args[2]);
//
//        // Initialize SparkSession
//        SparkSession spark = SparkSession.builder()
//                .appName("Sampling Method on Hadoop")
//                .getOrCreate();
//
//        // Read CSV file into DataFrame
//        Dataset<Row> df = spark.read().format("csv").option("header", "true").load(inputPath);
//
//        // Sample the data
//        List<Double> sampledData = sampleData(df);
//
//        // Perform sampling
//        SamplingMethod sampler = new SamplingMethod(numPartitions); // Using input numPartitions
//        long startTime = System.currentTimeMillis(); // Start time
//
//        List<Double> partitions = sampler.quantileBasedSampling(sampledData);
//
//        long endTime = System.currentTimeMillis(); // End time
//        long executionTime = endTime - startTime; // Calculate execution time in ms
//
//        // Prepare result for saving
//        String message = "Sampling completed successfully";
//
//        // Create JSON result
//        JSONObject resultJson = new JSONObject();
//        resultJson.put("message", message);
//        resultJson.put("executionTime", executionTime); // in milliseconds
//        resultJson.put("numberOfPartitions", partitions.size());
//        resultJson.put("partitions", partitions);
//
//        // Save result as JSON to HDFS
//        saveResultToJson(spark, outputPath, resultJson);
//
//        System.out.println("Sampling result saved to " + outputPath);
//
//        // Stop Spark session
//        spark.stop();
//    }
//
//    /**
//     * Samples data from the DataFrame.
//     */
//    private static List<Double> sampleData(Dataset<Row> df) {
//        // Example of sampling 10% of the dataset randomly
//        Dataset<Row> sampleDF = df.sample(false, 0.1);
//
//        // Convert to List of Doubles
//        List<Double> data = new ArrayList<>();
//        sampleDF.collectAsList().forEach(row -> {
//            data.add(Double.parseDouble(row.getString(0))); // Assuming numeric data in the first column
//        });
//
//        return data;
//    }
//
//    /**
//     * Saves the JSON result to a file on HDFS using Spark DataFrame API.
//     */
//    private static void saveResultToJson(SparkSession spark, String outputPath, JSONObject resultJson) throws Exception {
//        // Convert the JSON result to a DataFrame
//        List<Row> resultList = new ArrayList<>();
//        resultList.add(RowFactory.create(resultJson.toString()));
//
//        // Define the schema for the DataFrame
//        StructType schema = new StructType()
//                .add("result", DataTypes.StringType);
//
//        // Create a DataFrame from the list of JSON objects
//        Dataset<Row> resultDF = spark.createDataFrame(resultList, schema);
//
//        // Save the DataFrame as JSON to HDFS
//        resultDF.write().mode(SaveMode.Overwrite).json(outputPath);
//    }
}
