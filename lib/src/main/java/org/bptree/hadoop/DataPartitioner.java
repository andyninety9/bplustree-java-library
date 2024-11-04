package org.bptree.hadoop;


/**
 * DataPartitioner defines the contract for partitioning data into different categories for B+ Tree construction.
 *
 * @param <T> the type of data being partitioned
 */
public interface DataPartitioner<T> {

    /**
     * Determines the key for the partition to which a value belongs.
     *
     * @param value the value to evaluate
     * @return the partition key for the given value
     */
    String partitionKey(T value);
}