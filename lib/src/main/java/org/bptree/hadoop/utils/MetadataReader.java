package org.bptree.hadoop.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.bptree.hadoop.models.MetadataContainer;
import org.bptree.hadoop.models.SubtreeMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class MetadataReader {
    private static final Logger logger = LoggerFactory.getLogger(MetadataReader.class);

    public static List<SubtreeMetadata> readMetadata(String metadataPath) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(metadataPath);

        if (!fs.exists(path)) {
            throw new IOException("Metadata file not found at: " + metadataPath);
        }

        try (FSDataInputStream inputStream = fs.open(path)) {
            ObjectMapper objectMapper = new ObjectMapper();
            MetadataContainer container = objectMapper.readValue((InputStream) inputStream, MetadataContainer.class);

            if (container.getMetadata() == null || container.getMetadata().isEmpty()) {
                throw new IOException("Metadata file is empty or invalid.");
            }

            return container.getMetadata();
        }
    }
}
