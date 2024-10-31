package org.bptree.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;

/**
 * Utility class providing optimized multi-threaded merge sort.
 */
public class SortUtils {

    // ForkJoinPool to manage threads efficiently
    private static final ForkJoinPool FORK_JOIN_POOL = new ForkJoinPool();

    /**
     * Sorts a list of elements using an optimized multi-threaded merge sort.
     *
     * @param <T>  the type of elements in the list, must implement Comparable
     * @param data the list of elements to sort
     * @return the sorted list
     */
    public static <T extends Comparable<? super T>> List<T> mergeSort(List<T> data) {
        int threshold = calculateThreshold(data.size(), Runtime.getRuntime().availableProcessors());

        // Invoke the ForkJoinPool to perform merge sort
        return FORK_JOIN_POOL.invoke(new MergeSortTask<>(data, threshold));
    }

    /**
     * Calculates the optimal threshold based on data size and available CPU cores.
     *
     * @param dataSize the size of the dataset
     * @param cpuCores the number of available CPU cores
     * @return the calculated threshold
     */
    private static int calculateThreshold(int dataSize, int cpuCores) {
        // Ensure we don't divide too small; return at least 10,000 as a fallback.
        return Math.max(10_000, dataSize / (cpuCores * 2));
    }

    /**
     * A ForkJoinTask that performs optimized multi-threaded merge sort.
     *
     * @param <T> the type of elements to sort
     */
    private static class MergeSortTask<T extends Comparable<? super T>> extends RecursiveTask<List<T>> {
        private final List<T> data;
        private final int threshold;

        MergeSortTask(List<T> data, int threshold) {
            this.data = data;
            this.threshold = threshold;
        }

        @Override
        protected List<T> compute() {
            if (data.size() <= threshold) {
                data.sort(Comparable::compareTo);  // Sort sequentially
                return data;
            }

            int mid = data.size() / 2;
            List<T> left = data.subList(0, mid);
            List<T> right = data.subList(mid, data.size());

            MergeSortTask<T> leftTask = new MergeSortTask<>(new ArrayList<>(left), threshold);
            MergeSortTask<T> rightTask = new MergeSortTask<>(new ArrayList<>(right), threshold);

            leftTask.fork();  // Execute left task asynchronously
            List<T> sortedRight = rightTask.compute();  // Compute right task in the current thread
            List<T> sortedLeft = leftTask.join();  // Wait for the left task to complete

            return merge(sortedLeft, sortedRight);
        }
    }

    /**
     * Merges two sorted lists into one sorted list.
     *
     * @param <T>   the type of elements to merge
     * @param left  the first sorted list
     * @param right the second sorted list
     * @return the merged sorted list
     */
    private static <T extends Comparable<? super T>> List<T> merge(List<T> left, List<T> right) {
        List<T> result = new ArrayList<>(left.size() + right.size());
        int i = 0, j = 0;

        while (i < left.size() && j < right.size()) {
            if (left.get(i).compareTo(right.get(j)) <= 0) {
                result.add(left.get(i++));
            } else {
                result.add(right.get(j++));
            }
        }

        while (i < left.size()) {
            result.add(left.get(i++));
        }

        while (j < right.size()) {
            result.add(right.get(j++));
        }

        return result;
    }
}
