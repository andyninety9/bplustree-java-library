package org.bptree.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
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
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Main class for configuring and executing the BPlusTree Hadoop MapReduce job.
 * It initializes the MapReduce job, tracks execution times, and displays job output upon completion.
 */
public class BPlusTreeJob {

    /**
     * Entry point for executing the BPlusTree Hadoop MapReduce job.
     *
     * @param args Command-line arguments specifying the input and output paths
     * @throws Exception if there is an error during job configuration or execution
     */
    public static void main(String[] args) throws Exception {
        long totalStartTime = System.currentTimeMillis();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "BPlusTree Job for Distributed B+ Tree Construction");
        job.setJarByClass(BPlusTreeJob.class);

        job.setMapperClass(BPlusTreeMapper.MapPhase.class);
        job.setReducerClass(BPlusTreeReducer.ReducePhase.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outputPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outputPath);

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        boolean success = job.waitForCompletion(true);
        if (success) {
            generateMetadata(fs, outputPath, conf);
        } else {
            System.err.println("Hadoop Job failed.");
        }

        long totalEndTime = System.currentTimeMillis();
        System.out.println("Total Execution Time: " + (totalEndTime - totalStartTime) + " ms");
    }

    /**
     * Generates a metadata file based on the output of reducers.
     *
     * @param fs         the FileSystem instance
     * @param outputPath the output path of the reducers
     * @param conf       Hadoop configuration
     * @throws Exception if an error occurs during metadata creation
     */
    private static void generateMetadata(FileSystem fs, Path outputPath, Configuration conf) throws Exception {
        Path metadataPath = new Path(outputPath, "metadata.json");
        List<String> metadataEntries = new ArrayList<>();

        System.out.println("Reading reducer output files from: " + outputPath);
        for (FileStatus status : fs.listStatus(outputPath)) {
            if (status.isFile() && status.getPath().getName().startsWith("part-r-")) {
                System.out.println("Processing file: " + status.getPath());
                try (FSDataInputStream in = fs.open(status.getPath());
                     BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        System.out.println("Read line: " + line);
                        metadataEntries.add(line);
                    }
                } catch (Exception e) {
                    System.err.println("Error reading reducer output file: " + status.getPath());
                    e.printStackTrace();
                }
            }
        }

        if (metadataEntries.isEmpty()) {
            System.out.println("No metadata entries found. Check reducer output or processing.");
            return;
        }

        System.out.println("Writing metadata to: " + metadataPath);
        try (FSDataOutputStream out = fs.create(metadataPath);
             OutputStreamWriter writer = new OutputStreamWriter(out, StandardCharsets.UTF_8)) {
            writer.write("{ \"metadata\": [\n");

            // Regex pattern to extract details from each line
            String pattern = "B\\+ Tree stored for key (\\d+)\\s+Path: (\\S+), Min: (\\d+), Max: (\\d+), Height: (\\d+), Elements: (\\d+)";
            Pattern regex = Pattern.compile(pattern);

            for (int i = 0; i < metadataEntries.size(); i++) {
                try {
                    Matcher matcher = regex.matcher(metadataEntries.get(i));
                    if (matcher.find()) {
                        String partitionKey = matcher.group(1);
                        String path = matcher.group(2);
                        String minValue = matcher.group(3);
                        String maxValue = matcher.group(4);
                        String height = matcher.group(5);
                        String elements = matcher.group(6);

                        writer.write("  {\n");
                        writer.write("    \"partition_key\": \"" + partitionKey + "\",\n");
                        writer.write("    \"path\": \"" + path + "\",\n");
                        writer.write("    \"min_value\": " + minValue + ",\n");
                        writer.write("    \"max_value\": " + maxValue + ",\n");
                        writer.write("    \"height\": " + height + ",\n");
                        writer.write("    \"elements\": " + elements + "\n");
                        writer.write("  }");
                        if (i < metadataEntries.size() - 1) writer.write(",");
                        writer.write("\n");
                    } else {
                        System.err.println("Line does not match pattern: " + metadataEntries.get(i));
                    }
                } catch (Exception e) {
                    System.err.println("Error processing metadata entry: " + metadataEntries.get(i));
                    e.printStackTrace();
                }
            }
            writer.write("]}\n");
        } catch (Exception e) {
            System.err.println("Error writing metadata file: " + metadataPath);
            e.printStackTrace();
        }

        System.out.println("Metadata file created at: " + metadataPath);
    }
}
