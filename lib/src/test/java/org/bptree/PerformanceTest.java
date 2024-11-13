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

public class PerformanceTest {

    // Path to the dataset (relative to src/test/resources)
    private static final String DATASET_PATH = "/dataset/dataset_300mb.csv";
    private static final int TREE_ORDER = 100;  // Order of the BPlusTree

    private static List<Long> keys;  // Store the dataset keys for all tests

    /**
     * Load the dataset before all tests.
     * This method is executed only once for the entire test class.
     */

    @BeforeAll
    public static void loadData() throws IOException, URISyntaxException {
        // Step 1: Convert resource path to absolute file path
        String absolutePath = Paths.get(
                PerformanceTest.class.getResource(DATASET_PATH).toURI()
        ).toString();

        // Step 2: Read data from the CSV file using FileUtils
        List<String[]> csvData = FileUtils.readCSV(absolutePath, ",");
        assertFalse(csvData.isEmpty(), "The dataset should not be empty.");

        // Extract the first column as a list of Long values
        keys = csvData.stream()
                .map(row -> Long.parseLong(row[0].trim()))
                .collect(Collectors.toList());

        System.out.println("Dataset loaded successfully. Number of keys: " + keys.size());
    }

    /**
     * Test to measure the performance of sorting and tree construction separately.
     */
    @Test
    public void testSortAndBottomUpPerformance() throws InterruptedException, ExecutionException {
        // Step 1: Measure the time for sorting the keys
        long sortStartTime = System.nanoTime();
        List<Long> sortedKeys = SortUtils.mergeSort(keys);  // Sort the keys
        long sortEndTime = System.nanoTime();

        long sortDurationMillis = (sortEndTime - sortStartTime) / 1_000_000;
        System.out.println("Sorting took: " + sortDurationMillis + " ms");

        // Step 2: Create a BPlusTree with the specified order
        BPlusTree<Long> bplusTree = new BPlusTree<>(TREE_ORDER);

        // Step 3: Measure the time for bottom-up tree construction
        long buildStartTime = System.nanoTime();
        bplusTree.bottom_up_method(sortedKeys);  // Build the tree
        long buildEndTime = System.nanoTime();

        long buildDurationMillis = (buildEndTime - buildStartTime) / 1_000_000;
        System.out.println("Bottom-up tree construction took: " + buildDurationMillis + " ms");

        // Step 4: Validate the tree structure
        assertNotNull(bplusTree.getRoot(), "The root node should not be null.");
        assertTrue(bplusTree.getHeight() > 0, "The tree height should be greater than 0.");
        System.out.println("Tree height: " + bplusTree.getHeight());
    }
}
