package org.bptree.hadoop.common;

/**
 * Constant values used for partitioning keys and defining keys for the B+ Tree library.
 */
public interface Constant {

    /**
     * Maximum value for partition 1.
     */
    int MAX_PARTITION1 = 2000000;

    /**
     * Maximum value for partition 2.
     */
    int MAX_PARTITION2 = 4000000;

    /**
     * Maximum value for partition 3.
     */
    int MAX_PARTITION3 = 6000000;

    /**
     * Maximum value for partition 4.
     */
    int MAX_PARTITION4 = 8000000;

    /**
     * Key identifier for partition 1.
     */
    String KEY1 = "1";

    /**
     * Key identifier for partition 2.
     */
    String KEY2 = "2";

    /**
     * Key identifier for partition 3.
     */
    String KEY3 = "3";

    /**
     * Key identifier for partition 4.
     */
    String KEY4 = "4";

    /**
     * Key identifier for partition 5.
     */
    String KEY5 = "5";
}
