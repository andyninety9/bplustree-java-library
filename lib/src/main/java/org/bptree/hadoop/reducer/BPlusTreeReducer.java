package org.bptree.hadoop.reducer;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.bptree.BPlusTree;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * BPlusTreeReducer is a custom Reducer class that constructs B+ Trees
 * directly from partitioned data received from Mapper.
 */
public class BPlusTreeReducer {

    /**
     * ReducePhase class builds B+ Trees for each partition key without further splitting.
     */
    public static class ReducePhase extends Reducer<Text, IntWritable, Text, Text> {

        private static final int B_PLUS_TREE_ORDER = 100; // Order of the B+ Tree

        /**
         * Processes each partition key and its associated values to build a B+ Tree.
         *
         * @param key     the partition key.
         * @param values  the list of values associated with this partition key.
         * @param context the Hadoop Context for writing results and tracking job progress.
         */
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            // Collect all values and track statistics in one pass
            List<Integer> valueList = new ArrayList<>();
            int minValue = Integer.MAX_VALUE;
            int maxValue = Integer.MIN_VALUE;

            for (IntWritable value : values) {
                int currentValue = value.get();
                valueList.add(currentValue);
                minValue = Math.min(minValue, currentValue);
                maxValue = Math.max(maxValue, currentValue);
            }

            // Skip processing if there are no values
            if (valueList.isEmpty()) {
                System.err.println("No values for key: " + key.toString());
                return;
            }

            // Build B+ Tree with all sorted values
            BPlusTree<Integer> bPlusTree = new BPlusTree<>(B_PLUS_TREE_ORDER);
            try {
                bPlusTree.bottom_up_method(valueList);
            } catch (ExecutionException e) {
                System.err.println("Error building B+ Tree for key: " + key.toString() + " - " + e.getMessage());
                throw new RuntimeException("Failed to build B+ Tree", e);
            }

            // Serialize the tree
            byte[] serializedTree;
            try {
                serializedTree = serializeBPlusTree(bPlusTree);
                if (serializedTree == null) {
                    throw new IOException("Serialization returned null for key: " + key.toString());
                }
            } catch (IOException e) {
                System.err.println("Serialization failed for B+ Tree with key: " + key.toString() + " - " + e.getMessage());
                e.printStackTrace();
                throw new IOException("Failed to serialize B+ Tree for key: " + key.toString(), e);
            }

            // Save to HDFS with unique filename
            String treePath = "/listTree/" + key.toString() + "/tree_serialized_" + UUID.randomUUID();
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);

            try (FSDataOutputStream outputStream = fs.create(new Path(treePath))) {
                outputStream.write(serializedTree);
            } catch (IOException e) {
                System.err.println("Failed to write B+ Tree to HDFS for key: " + key.toString() + " - " + e.getMessage());
                e.printStackTrace();
                throw new IOException("Failed to write serialized B+ Tree to HDFS", e);
            }

            // Write metadata to context
            context.write(
                    new Text("B+ Tree stored for key " + key.toString()),
                    new Text("Path: " + treePath +
                            ", Min: " + minValue +
                            ", Max: " + maxValue +
                            ", Height: " + bPlusTree.getHeight() +
                            ", Elements: " + valueList.size())
            );
        }

        /**
         * Serializes a B+ Tree to a byte array.
         *
         * @param bPlusTree the B+ Tree to serialize.
         * @return a byte array representing the serialized B+ Tree.
         * @throws IOException if an I/O error occurs during serialization
         */
        private byte[] serializeBPlusTree(BPlusTree<Integer> bPlusTree) throws IOException {
            try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                 ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)) {
                objectOutputStream.writeObject(bPlusTree);
                objectOutputStream.flush();
                return byteArrayOutputStream.toByteArray();
            } catch (IOException e) {
                System.err.println("Error serializing B+ Tree: " + e.getMessage());
                e.printStackTrace();
                throw new IOException("Failed to serialize B+ Tree", e);
            }
        }
    }
}