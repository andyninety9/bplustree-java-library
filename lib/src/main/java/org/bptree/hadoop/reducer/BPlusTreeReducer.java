package org.bptree.hadoop.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.bptree.BPlusTree;

import java.io.IOException;
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
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // Collect all values for the given partition key
            List<Integer> valueList = new ArrayList<>();
            for (IntWritable value : values) {
                valueList.add(value.get());
            }

            // Skip processing if there are no values for the given key
            if (valueList.isEmpty()) {
                System.err.println("No values for key: " + key.toString());
                return;
            }

            // Initialize a B+ Tree instance and build it using the bottom-up method
            BPlusTree<Integer> bPlusTree = new BPlusTree<>(B_PLUS_TREE_ORDER);
            try {
                // Build the B+ Tree from the collected values
                bPlusTree.bottom_up_method(valueList);
            } catch (ExecutionException e) {
                // Handle errors that occur during tree construction
                System.err.println("Error building B+ Tree for key: " + key.toString() + " - " + e.getMessage());
                throw new RuntimeException("Failed to build B+ Tree", e);
            }

            // Calculate minimum and maximum values in the current partition
            int minValue = valueList.stream().min(Integer::compareTo).orElse(Integer.MIN_VALUE);
            int maxValue = valueList.stream().max(Integer::compareTo).orElse(Integer.MAX_VALUE);

            // Commenting out serialization step
            /*
            // Serialize the B+ Tree into a byte array for storage
            byte[] serializedTree;
            try {
                // Convert the B+ Tree object into a byte array
                serializedTree = serializeBPlusTree(bPlusTree);
            } catch (IOException e) {
                // Handle serialization errors
                System.err.println("Serialization failed for B+ Tree with key: " + key.toString() + " - " + e.getMessage());
                e.printStackTrace();
                throw new IOException("Failed to serialize B+ Tree for key: " + key.toString(), e);
            }
            */

            // Define a path for HDFS storage (intentionally leaving the path empty for this requirement)
            String path = ""; // The path field is intentionally left empty

            // Commenting out HDFS write step
            /*
            // Save the serialized B+ Tree to HDFS
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            try (FSDataOutputStream outputStream = fs.create(new Path("/listTree/" + key.toString() + "/tree_serialized_" + UUID.randomUUID()))) {
                // Write the serialized B+ Tree to HDFS
                outputStream.write(serializedTree);
            } catch (IOException e) {
                // Handle errors during HDFS write operations
                System.err.println("Failed to write B+ Tree to HDFS for key: " + key.toString() + " - " + e.getMessage());
                e.printStackTrace();
                throw new IOException("Failed to write serialized B+ Tree to HDFS", e);
            }
            */

            // Write metadata to the Hadoop context with an empty Path field
            context.write(new Text("B+ Tree stored for key " + key.toString()),
                    new Text("Path: " + path + ", Min: " + minValue + ", Max: " + maxValue +
                            ", Height: " + bPlusTree.getHeight() + ", Elements: " + valueList.size()));
        }

        /**
         * Serializes a B+ Tree to a byte array.
         *
         * @param bPlusTree the B+ Tree to serialize.
         * @return a byte array representing the serialized B+ Tree.
         * @throws IOException if an I/O error occurs during serialization
         */
        private byte[] serializeBPlusTree(BPlusTree<Integer> bPlusTree) throws IOException {
            // Serialization logic is commented out as it is not needed in this case
            /*
            try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                 ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)) {
                // Serialize the B+ Tree object into the byte array
                objectOutputStream.writeObject(bPlusTree);
                objectOutputStream.flush(); // Ensure all data is written
                return byteArrayOutputStream.toByteArray();
            } catch (IOException e) {
                // Handle errors during serialization
                System.err.println("Error serializing B+ Tree: " + e.getMessage());
                e.printStackTrace();
                throw new IOException("Failed to serialize B+ Tree", e);
            }
            */
            return null; // Return null as serialization is skipped
        }
    }
}
