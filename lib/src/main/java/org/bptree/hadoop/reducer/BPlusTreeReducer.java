package org.bptree.hadoop.reducer;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.bptree.BPlusTree;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
            List<Integer> valueList = new ArrayList<>();
            for (IntWritable value : values) {
                valueList.add(value.get());
            }

            if (valueList.isEmpty()) {
                System.err.println("No values for key: " + key.toString());
                return;
            }

            // Initialize and build the B+ Tree with data
            BPlusTree<Integer> bPlusTree = new BPlusTree<>(B_PLUS_TREE_ORDER);
            try {
                bPlusTree.bottom_up_method(valueList);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }

            // Write the height of the B+ Tree and the number of elements to the context
            context.write(new Text("B+ Tree built for key " + key.toString()),
                    new Text("Height: " + bPlusTree.getHeight() + ", Elements: " + valueList.size()));
        }
    }
}
