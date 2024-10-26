package org.bptree.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A utility class for file-related operations.
 * Provides reusable methods to read text and CSV files.
 */
public class FileUtils {

    /**
     * Reads all lines from a text file and returns them as a list of strings.
     *
     * @param filePath the path to the file to read
     * @return a list of strings representing the lines in the file
     * @throws IOException if an I/O error occurs
     */
    public static List<String> readAllLines(String filePath) throws IOException {
        List<String> lines = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
        }
        return lines;
    }

    /**
     * Reads a CSV file and returns its content as a list of string arrays.
     *
     * @param filePath the path to the CSV file
     * @return a list of string arrays, where each array is a row in the CSV
     * @throws IOException if an I/O error occurs
     */
    public static List<String[]> readCSV(String filePath, String regex) throws IOException {
        List<String[]> data = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                data.add(line.split(regex));  // Split each line by user's regex
            }
        }
        return data;
    }
}
