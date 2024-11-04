package org.bptree;

import org.bptree.utils.FileUtils;
import org.bptree.utils.SortUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

public class PerformanceParallelSearchTest {

    // Path to the dataset (relative to src/test/resources)
    private static final String DATASET_PATH = "/dataset/dataset_100mb.csv";
    private static final int TREE_ORDER = 100;  // Order of the BPlusTree

    private static List<Long> keys;  // Store the dataset keys for all tests
    private static BPlusTree<Long> bplusTree;

    /**
     * Load the dataset and build the B+ Tree before all tests.
     * This method is executed only once for the entire test class.
     */
    @Disabled
    @BeforeAll
    public static void setup() throws IOException, URISyntaxException, InterruptedException, ExecutionException {
        // Step 1: Convert resource path to absolute file path
        String absolutePath = Paths.get(
                PerformanceParallelSearchTest.class.getResource(DATASET_PATH).toURI()
        ).toString();

        // Step 2: Read data from the CSV file using FileUtils
        List<String[]> csvData = FileUtils.readCSV(absolutePath, ",");
        assertFalse(csvData.isEmpty(), "The dataset should not be empty.");

        // Extract the first column as a list of Long values
        keys = csvData.stream()
                .map(row -> Long.parseLong(row[0].trim()))
                .collect(Collectors.toList());;

        System.out.println("Dataset loaded successfully. Number of keys: " + keys.size());

        // Step 3: Sort the keys
        List<Long> sortedKeys = SortUtils.mergeSort(keys);

        // Step 4: Build the B+ Tree
        bplusTree = new BPlusTree<>(TREE_ORDER);
        bplusTree.bottom_up_method(sortedKeys);
    }

    /**
     * Test to measure the performance of the parallel search.
     */

    @Disabled
    @Test
    public void testParallelSearchPerformance() throws InterruptedException, ExecutionException {
        assertNotNull(bplusTree.getRoot(), "The root node should not be null.");
        assertTrue(bplusTree.getHeight() > 0, "The tree height should be greater than 0.");

        // Choose a key to search for (e.g., a key that exists in the dataset)
        Long keyToSearch = keys.get(keys.size() / 2);

        // Measure the time taken for the parallel search
        long searchStartTime = System.nanoTime();
        boolean searchResult = bplusTree.parallelSearch(keyToSearch);
        long searchEndTime = System.nanoTime();

        long searchDurationMillis = (searchEndTime - searchStartTime) / 1_000_000;
        System.out.println("Parallel search took: " + searchDurationMillis + " ms");
        System.out.println(searchResult ? "Found" : "Not found.");

        // Verify the search result
        assertTrue(searchResult, "The key should be found in the B+ Tree.");
    }
}
