package org.bptree.hadoop.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bptree.hadoop.common.Constant;

import java.io.IOException;

/**
 * BPlusTreeMapper is a custom Mapper class for processing data into partitions
 * that are assigned to different B+ Tree segments.
 * <p>
 * Each data item is mapped to a specific partition key based on the value
 * to balance the B+ Tree construction process across multiple nodes.
 */
public class BPlusTreeMapper {

    /**
     * MapPhase class is the core Mapper logic.
     * It reads lines of data, parses each line to an integer,
     * assigns it to a partition key based on defined ranges in {@link Constant},
     * and writes the partitioned data to the context for Reducer processing.
     */
    public static class MapPhase extends Mapper<LongWritable, Text, Text, IntWritable> {

        /**
         * The map method processes each line of input data.
         *
         * @param key     the byte offset of the line within the file (not used).
         * @param value   the line content as a Text object.
         * @param context the Hadoop Context to write key-value pairs for Reducer.
         */
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                // Convert the line to a String and trim any whitespace.
                String line = value.toString().trim();
                // Parse the line content to an integer.
                int eachLineData = Integer.parseInt(line);

                // Determine the partition key based on the integer value.
                Text partitionKey = new Text(determineKey(eachLineData));

                // Write the key-value pair (partition key, data item) to context.
                context.write(partitionKey, new IntWritable(eachLineData));

            } catch (NumberFormatException e) {
                // Log any lines that cannot be parsed to an integer.
                System.err.println("Error parsing number format: " + value.toString());
            }
        }

        /**
         * Determines the partition key for a given value based on predefined ranges.
         *
         * @param value the data item to be partitioned.
         * @return the partition key as a String.
         */
        private static String determineKey(int value) {
            if (value < Constant.MAX_PARTITION1) {
                return Constant.KEY1;
            } else if (value < Constant.MAX_PARTITION2) {
                return Constant.KEY2;
            } else if (value < Constant.MAX_PARTITION3) {
                return Constant.KEY3;
            } else if (value < Constant.MAX_PARTITION4) {
                return Constant.KEY4;
            } else {
                return Constant.KEY5;
            }
        }
    }
}



