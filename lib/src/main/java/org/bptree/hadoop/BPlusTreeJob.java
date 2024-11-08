package org.bptree.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.bptree.hadoop.mapper.BPlusTreeMapper;
import org.bptree.hadoop.reducer.BPlusTreeReducer;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * Main class for configuring and executing the BPlusTree Hadoop MapReduce job.
 * It initializes the MapReduce job, tracks execution times, and displays job output upon completion.
 */
public class BPlusTreeJob {

    /**
     * Entry point for executing the BPlusTree Hadoop MapReduce job.
     *
     * This method configures the job parameters, sets up input and output paths,
     * and tracks execution times for both the job itself and the overall process.
     *
     * @param args Command-line arguments specifying the input and output paths
     * @throws Exception if there is an error during job configuration or execution
     */
    public static void main(String[] args) throws Exception {
        // Record the total execution start time
        long totalStartTime = System.currentTimeMillis();

        // Hadoop configuration setup
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "BPlusTree Job for Distributed B+ Tree Construction");
        job.setJarByClass(BPlusTreeJob.class);

        // Set Mapper and Reducer classes for processing data
        job.setMapperClass(BPlusTreeMapper.MapPhase.class);
        job.setReducerClass(BPlusTreeReducer.ReducePhase.class);

        // Define output key and value types for the Mapper
        job.setMapOutputKeyClass(Text.class); // Output key type from Mapper
        job.setMapOutputValueClass(IntWritable.class); // Output value type from Mapper

        // Define output key and value types for the Reducer (final output)
        job.setOutputKeyClass(Text.class); // Output key type from Reducer
        job.setOutputValueClass(Text.class); // Output value type from Reducer

        // Set the input and output paths based on command-line arguments
        FileInputFormat.addInputPath(job, new Path(args[0])); // Input path for dataset
        Path outputPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outputPath); // Output path for results

        // Delete the output path if it already exists to prevent job failure
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        try {
            // Record start time for the job execution
            long jobStartTime = System.currentTimeMillis();

            // Execute the Hadoop job and capture success status
            boolean success = job.waitForCompletion(true);

            // Record end time for the job execution
            long jobEndTime = System.currentTimeMillis();
            System.out.println("Hadoop Job Execution Time: " + (jobEndTime - jobStartTime) + " ms");

            if (success) {
                // Job succeeded; proceed to read and print output files from HDFS
                for (FileStatus status : fs.listStatus(outputPath)) {
                    if (status.isFile()) {
                        // Read each file and print its content to the console
                        try (FSDataInputStream in = fs.open(status.getPath());
                             BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
                            String line;
                            while ((line = reader.readLine()) != null) {
                                System.out.println(line);
                            }
                        }
                    }
                }
            } else {
                System.err.println("Hadoop Job failed.");
            }
        } catch (Exception e) {
            // Exception handling for errors during job execution
            System.err.println("Error occurred while running the Hadoop job: " + e.getMessage());
            e.printStackTrace();
        }

        // Record the total execution end time
        long totalEndTime = System.currentTimeMillis();
        System.out.println("Total Execution Time: " + (totalEndTime - totalStartTime) + " ms");
    }
}
