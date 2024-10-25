package org.bptree;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Represents a B+ Tree with configurable order and generic data type.
 *
 * @param <T> the type of keys stored in the tree nodes
 * @author andymai
 * @version 1.1
 */
public class BPlusTree<T> {
    private Node<T> root;  // Root node of the B+ Tree
    private final int order;  // Order (degree) of the B+ Tree

    /**
     * Constructs a BPlusTree with the specified order.
     *
     * @param order the order of the B+ tree, must be at least 3
     * @throws IllegalArgumentException if the order is less than 3
     */
    public BPlusTree(int order) {
        if (order < 3) {
            throw new IllegalArgumentException("Order must be at least 3.");
        }
        this.order = order;
        this.root = null;
    }

    /**
     * Constructs the B+ Tree from bottom to top using the given list of data.
     *
     * @param listData a list of keys to insert into the tree
     * @throws InterruptedException if the thread is interrupted while waiting
     * @throws ExecutionException if an exception occurs during task execution
     */
    public void bottom_up_method(List<T> listData) throws InterruptedException, ExecutionException {
        int availableThreads = Math.max(1, Runtime.getRuntime().availableProcessors() - 2);
        ExecutorService executor = Executors.newFixedThreadPool(availableThreads);
        List<Future<Node<T>>> futures = new ArrayList<>();

        try {
            // Build leaf nodes concurrently
            for (int i = 0; i < listData.size(); i += (this.order - 1)) {
                int end = Math.min(i + (this.order - 1), listData.size());
                List<T> chunk = listData.subList(i, end);
                futures.add(executor.submit(construct_leaf_level(chunk)));
            }

            List<Node<T>> leafNodes = new ArrayList<>();
            for (Future<Node<T>> future : futures) {
                leafNodes.add(future.get());  // Collect leaf nodes
            }

            // Link leaf nodes in sequence
            link_leaf_nodes(leafNodes);

            // Build the internal levels recursively
            this.root = build_internal_levels(leafNodes);
        } finally {
            executor.shutdown();
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                System.err.println("Executor did not terminate in time.");
            }
        }
    }

    /**
     * Creates a Callable task that constructs a leaf node from the given data.
     *
     * @param data a chunk of keys to be stored in the leaf node
     * @return a Callable task that returns the constructed leaf node
     */
    private Callable<Node<T>> construct_leaf_level(List<T> data) {
        return () -> {
            Node<T> node = new Node<>(true);  // Create a new leaf node
            node.getKeys().addAll(data);  // Add all keys to the node
            return node;
        };
    }

    /**
     * Links the given leaf nodes in sequence using the next pointer.
     *
     * @param leafNodes the list of leaf nodes to link
     */
    private void link_leaf_nodes(List<Node<T>> leafNodes) {
        for (int i = 0; i < leafNodes.size() - 1; i++) {
            leafNodes.get(i).setNext(leafNodes.get(i + 1));  // Link to the next leaf node
        }
    }

    /**
     * Builds the internal levels recursively from the given child nodes.
     *
     * @param childNodes the list of child nodes
     * @return the root node of the constructed level
     */
    private Node<T> build_internal_levels(List<Node<T>> childNodes) {
        if (childNodes.size() == 1) {
            return childNodes.get(0);  // If only one node remains, it becomes the root
        }

        List<Node<T>> internalNodes = new ArrayList<>();
        Node<T> currentNode = new Node<>(false);  // Create a new internal node

        for (int i = 0; i < childNodes.size(); i++) {
            currentNode.getChildren().add(childNodes.get(i));  // Add child to current internal node

            if (i % (order - 1) == (order - 2) || i == childNodes.size() - 1) {
                internalNodes.add(currentNode);  // Add completed internal node
                currentNode = new Node<>(false);  // Create a new internal node
            }
        }

        System.out.println("Built Internal Level with " + internalNodes.size() + " nodes.");
        return build_internal_levels(internalNodes);  // Recursively build the next level
    }

    /**
     * Calculates the height of the B+ Tree.
     *
     * @return the height of the tree
     */
    public int getHeight() {
        int height = 0;
        Node<T> currentNode = root;

        // Traverse down the tree until reaching a leaf node
        while (currentNode != null) {
            height++;
            currentNode = currentNode.isLeaf() ? null : currentNode.getChildren().get(0);
        }

        return height;
    }

    /**
     * Returns the root node of the B+ Tree.
     *
     * @return the root node
     */
    public Node<T> getRoot() {
        return root;
    }
}
