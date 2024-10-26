package org.bptree.utils;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class FileUtilsTest {

    @Test
    public void testReadAllLines() throws IOException {
        // Create a temporary file with some content
        Path tempFile = Files.createTempFile("test", ".txt");
        Files.writeString(tempFile, "Hello\nWorld\n");

        // Use FileUtils to read the file
        List<String> lines = FileUtils.readAllLines(tempFile.toString());

        // Validate the result
        assertEquals(2, lines.size(), "The file should contain 2 lines.");
        assertEquals("Hello", lines.get(0));
        assertEquals("World", lines.get(1));
    }

    @Test
    public void testReadCSV() throws IOException {
        // Create a temporary CSV file with some content
        Path tempFile = Files.createTempFile("test", ".csv");
        Files.writeString(tempFile, "Name,Age\nAlice,30\nBob,25\n");

        // Use FileUtils to read the CSV
        List<String[]> data = FileUtils.readCSV(tempFile.toString(), ",");

        // Validate the result
        assertEquals(3, data.size(), "The CSV should contain 3 rows.");
        assertArrayEquals(new String[]{"Name", "Age"}, data.get(0));
        assertArrayEquals(new String[]{"Alice", "30"}, data.get(1));
        assertArrayEquals(new String[]{"Bob", "25"}, data.get(2));
    }
}
