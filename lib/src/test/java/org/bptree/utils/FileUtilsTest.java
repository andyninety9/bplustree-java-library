package org.bptree.utils;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class for FileUtils, providing unit tests for file reading methods.
 * Ensures the correct behavior of file operations used within the B+ Tree library.
 */
public class FileUtilsTest {

    /**
     * Tests the FileUtils.readAllLines() method to verify that it correctly reads all lines
     * from a temporary text file.
     *
     * @throws IOException if an I/O error occurs while creating or reading the file.
     */
    @Test
    public void testReadAllLines() throws IOException {
        // Create a temporary file with sample content for testing
        Path tempFile = Files.createTempFile("test", ".txt");
        Files.write(tempFile, "Hello\nWorld\n".getBytes(StandardCharsets.UTF_8));

        // Use FileUtils to read all lines from the temporary file
        List<String> lines = FileUtils.readAllLines(tempFile.toString());

        // Verify that the file content is read as expected
        assertEquals(2, lines.size(), "The file should contain 2 lines.");
        assertEquals("Hello", lines.get(0), "First line should be 'Hello'.");
        assertEquals("World", lines.get(1), "Second line should be 'World'.");
    }

    /**
     * Tests the FileUtils.readCSV() method to verify that it correctly reads a CSV file
     * and returns rows of data as string arrays.
     *
     * This test is currently disabled to avoid unnecessary file operations during routine testing.
     * Enable this test as needed by removing the @Disabled annotation.
     *
     * @throws IOException if an I/O error occurs while creating or reading the file.
     */
    @Disabled
    @Test
    public void testReadCSV() throws IOException {
        // Create a temporary CSV file with sample content for testing
        Path tempFile = Files.createTempFile("test", ".csv");
        Files.write(tempFile, "Name,Age\nAlice,30\nBob,25\n".getBytes(StandardCharsets.UTF_8));

        // Use FileUtils to read CSV content from the temporary file
        List<String[]> data = FileUtils.readCSV(tempFile.toString(), ",");

        // Verify that the CSV content is parsed as expected
        assertEquals(3, data.size(), "The CSV should contain 3 rows.");
        assertArrayEquals(new String[]{"Name", "Age"}, data.get(0), "First row should contain column headers.");
        assertArrayEquals(new String[]{"Alice", "30"}, data.get(1), "Second row should match sample data for 'Alice'.");
        assertArrayEquals(new String[]{"Bob", "25"}, data.get(2), "Third row should match sample data for 'Bob'.");
    }
}
