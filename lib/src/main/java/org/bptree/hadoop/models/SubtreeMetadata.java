package org.bptree.hadoop.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;

public class SubtreeMetadata implements Serializable {
    @JsonProperty("partition_key")
    private String partitionKey;

    @JsonProperty("path")
    private String path;

    @JsonProperty("min_value")
    private int minValue;

    @JsonProperty("max_value")
    private int maxValue;

    @JsonProperty("height")
    private int height;

    @JsonProperty("elements")
    private int elements;

    // Constructor mặc định
    public SubtreeMetadata() {}

    // Constructor đầy đủ
    public SubtreeMetadata(String partitionKey, String path, int minValue, int maxValue, int height, int elements) {
        this.partitionKey = partitionKey;
        this.path = path;
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.height = height;
        this.elements = elements;
    }

    // Getters và Setters
    public String getPartitionKey() {
        return partitionKey;
    }

    public void setPartitionKey(String partitionKey) {
        this.partitionKey = partitionKey;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public int getMinValue() {
        return minValue;
    }

    public void setMinValue(int minValue) {
        this.minValue = minValue;
    }

    public int getMaxValue() {
        return maxValue;
    }

    public void setMaxValue(int maxValue) {
        this.maxValue = maxValue;
    }

    public int getHeight() {
        return height;
    }

    public void setHeight(int height) {
        this.height = height;
    }

    public int getElements() {
        return elements;
    }

    public void setElements(int elements) {
        this.elements = elements;
    }

    @Override
    public String toString() {
        return "SubtreeMetadata{" +
                "partitionKey='" + partitionKey + '\'' +
                ", path='" + path + '\'' +
                ", minValue=" + minValue +
                ", maxValue=" + maxValue +
                ", height=" + height +
                ", elements=" + elements +
                '}';
    }
}
