package org.bptree.hadoop.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.List;

public class MetadataContainer implements Serializable {
    @JsonProperty("metadata")
    private List<SubtreeMetadata> metadata;

    public List<SubtreeMetadata> getMetadata() {
        return metadata;
    }

    public void setMetadata(List<SubtreeMetadata> metadata) {
        this.metadata = metadata;
    }

    @Override
    public String toString() {
        return "MetadataContainer{" +
                "metadata=" + metadata +
                '}';
    }
}
