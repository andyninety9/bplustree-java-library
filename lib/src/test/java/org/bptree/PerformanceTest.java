package org.bptree;

import org.bptree.utils.FileUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

public class PerformanceTest {

    // Path to the dataset (relative to src/test/resources)
    private static final String DATASET_PATH = "/dataset/dataset_300mb.csv";
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
     * Function to count the number of elements in a CSV file
     */
    public static long countElementsInCSV(String filePath) throws IOException {
        // Read the CSV file using FileUtils
        List<String[]> csvData = FileUtils.readCSV(filePath, ",");
        return csvData.size();
    }

    /**
     * Test the number of elements in the dataset
     */
    @Test
    public void testCountElementsInDataset() throws IOException, URISyntaxException {
        // Step 1: Convert resource path to absolute file path
        String absolutePath = Paths.get(
                PerformanceTest.class.getResource(DATASET_PATH).toURI()
        ).toString();

        // Step 2: Count the elements in the CSV file
        long elementCount = countElementsInCSV(absolutePath);

        // Step 3: Assert that the dataset contains elements
        assertTrue(elementCount > 0, "The dataset should contain at least one element.");
        System.out.println("The number of elements in the dataset: " + elementCount);
    }
}
