package org.bptree.hadoop.search;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.bptree.BPlusTree;
import org.bptree.hadoop.models.SubtreeMetadata;
import org.bptree.hadoop.utils.MetadataReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class MetadataSearcher {
    private static final Logger logger = LoggerFactory.getLogger(MetadataSearcher.class);

    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Usage: MetadataSearcher <searchKey> <metadataPath> <outputPath>");
            System.exit(1);
        }

        String searchKeyArg = args[0];
        String metadataPath = args[1];
        String outputPath = args[2];

        long startTime = System.currentTimeMillis(); // Bắt đầu tính thời gian

        SparkSession spark = null;
        try {
            // Khởi tạo SparkSession
            spark = SparkSession.builder()
                    .appName("Metadata Searcher")
                    .getOrCreate();
            logger.info("SparkSession initialized successfully.");

            // Parse search key
            int searchKey = Integer.parseInt(searchKeyArg);
            logger.info("Parsed search key: {}", searchKey);

            // Read metadata
            List<SubtreeMetadata> metadataList = MetadataReader.readMetadata(metadataPath);
            logger.info("Successfully read metadata: {}", metadataList);

            // Find matching subtrees
            List<SubtreeMetadata> matchedSubtrees = metadataList.stream()
                    .filter(metadata -> metadata.getMinValue() <= searchKey && metadata.getMaxValue() >= searchKey)
                    .collect(Collectors.toList());
            logger.info("Matched Subtrees: {}", matchedSubtrees);

            // Search in each subtree
            boolean isFound = false;
            for (SubtreeMetadata subtreeMetadata : matchedSubtrees) {
                logger.info("Searching in subtree: {}", subtreeMetadata.getPath());
                BPlusTree<Integer> subtree = readSubtreeFromHDFS(subtreeMetadata.getPath());
                isFound = searchInSubtree(subtree, searchKey);
                if (isFound) {
                    logger.info("Search key {} found in subtree: {}", searchKey, subtreeMetadata.getPath());
                    break;
                }
            }

            long endTime = System.currentTimeMillis(); // Kết thúc tính thời gian
            long executionTime = endTime - startTime;

            // Save result to HDFS
            saveSearchResult(isFound, matchedSubtrees, searchKey, executionTime, outputPath);
            logger.info("Search result saved to: {}", outputPath);

        } catch (NumberFormatException e) {
            logger.error("Invalid search key format: {}. Must be an integer.", searchKeyArg, e);
        } catch (Exception e) {
            logger.error("Error during metadata search process", e);
        } finally {
            if (spark != null) {
                spark.stop();
                logger.info("SparkSession stopped.");
            }
        }
    }


    private static BPlusTree<Integer> readSubtreeFromHDFS(String path) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path hdfsPath = new Path(path);

        if (!fs.exists(hdfsPath)) {
            throw new Exception("Subtree file not found at: " + path);
        }

        try (InputStream in = fs.open(hdfsPath);
             ObjectInputStream ois = new ObjectInputStream(in)) {
            @SuppressWarnings("unchecked")
            BPlusTree<Integer> subtree = (BPlusTree<Integer>) ois.readObject();
            return subtree;
        }
    }

    private static boolean searchInSubtree(BPlusTree<Integer> subtree, int searchKey) {
        try {
            return subtree.parallelSearch(searchKey);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private static void saveSearchResult(boolean isFound, List<SubtreeMetadata> matchedSubtrees, int searchKey, long executionTime, String outputPath) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(outputPath);

        SearchResult result = new SearchResult(isFound, matchedSubtrees, searchKey, executionTime);

        try (OutputStream outputStream = fs.create(path, true)) {
            ObjectMapper objectMapper = new ObjectMapper();
            // Cấu hình pretty-print cho JSON
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(outputStream, result);
        }
    }



    private static class SearchResult {
        private boolean found;
        private List<SubtreeMetadata> subtrees;
        private int searchKey; // Giá trị người dùng cần tìm
        private long executionTime; // Thời gian thực hiện tìm kiếm (milliseconds)

        public SearchResult(boolean found, List<SubtreeMetadata> subtrees, int searchKey, long executionTime) {
            this.found = found;
            this.subtrees = subtrees;
            this.searchKey = searchKey;
            this.executionTime = executionTime;
        }

        public boolean isFound() {
            return found;
        }

        public List<SubtreeMetadata> getSubtrees() {
            return subtrees;
        }

        public int getSearchKey() {
            return searchKey;
        }

        public long getExecutionTime() {
            return executionTime;
        }
    }

}




