package org.bptree;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

/**
 * Represents a B+ Tree with configurable order and generic data type.
 *
 * @param <T> the type of keys stored in the tree nodes
 * @author andymai
 * @version 1.2
 */
public class BPlusTree<T extends Comparable<T>> {
    private Node<T> root;  // Root node of the B+ Tree
    private final int order;  // Order (degree) of the B+ Tree

    /**
     * Returns the root node of the B+ Tree.
     *
     * @return the root node
     */
    public Node<T> getRoot() {
        return root;
    }

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
        if (listData.isEmpty()) {
            throw new IllegalArgumentException("Input data list should not be empty.");
        }

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
     * @throws IllegalStateException if the child nodes list is empty
     */
    private Node<T> build_internal_levels(List<Node<T>> childNodes) {
        if (childNodes.isEmpty()) {
            throw new IllegalStateException("Child nodes list should not be empty.");
        }

        if (childNodes.size() == 1) {
            return childNodes.get(0);  // If only one node remains, it becomes the root
        }

        List<Node<T>> internalNodes = new ArrayList<>();
        Node<T> currentNode = new Node<>(false);  // Create a new internal node

        for (int i = 0; i < childNodes.size(); i++) {
            currentNode.getChildren().add(childNodes.get(i));  // Add child to current internal node

            // Add the key from the first element of the next child node to act as the separator key
            if (currentNode.getChildren().size() > 1 && i < childNodes.size() - 1) {
                if (!childNodes.get(i + 1).getKeys().isEmpty()) {
                    T key = childNodes.get(i + 1).getKeys().get(0);
                    currentNode.getKeys().add(key);
                }
            }

            // If the current node is full or we have reached the last child node, add the current node to the list
            if (currentNode.getChildren().size() == order || i == childNodes.size() - 1) {
                internalNodes.add(currentNode);
                currentNode = new Node<>(false);  // Create a new internal node
            }
        }

        if (internalNodes.isEmpty()) {
            throw new IllegalStateException("Internal nodes list should not be empty after building level.");
        }

        System.out.println("Built Internal Level with " + internalNodes.size() + " nodes.");
        return build_internal_levels(internalNodes);  // Recursively build the next level
    }

    /**
     * Searches for a specific key in the B+ Tree using multithreading for faster access.
     *
     * @param key the key to search for
     * @return true if the key is found, false otherwise
     */
    public boolean parallelSearch(T key) throws InterruptedException, ExecutionException {
        if (root == null) {
            return false;  // Tree is empty
        }

        ExecutorService executor = Executors.newCachedThreadPool();
        try {
            List<Callable<Boolean>> tasks = new ArrayList<>();
            Node<T> currentNode = root;

            // Traverse the tree from root to leaf
            while (!currentNode.isLeaf()) {
                int i = 0;
                while (i < currentNode.getKeys().size() && key.compareTo(currentNode.getKeys().get(i)) >= 0) {
                    i++;
                }
                currentNode = currentNode.getChildren().get(i);
            }

            // Create tasks to search for the key in each leaf node concurrently
            Node<T> leafNode = currentNode;
            while (leafNode != null) {
                Node<T> finalLeafNode = leafNode;
                tasks.add(() -> finalLeafNode.getKeys().contains(key));
                leafNode = leafNode.getNext();
            }

            // Execute tasks concurrently and return if any task finds the key
            List<Future<Boolean>> results = executor.invokeAll(tasks);
            for (Future<Boolean> result : results) {
                if (result.get()) {
                    return true;
                }
            }
        } finally {
            executor.shutdown();
        }

        return false;
    }

    /**
     * Searches for a specific key in the B+ Tree using a traditional sequential approach.
     *
     * @param key the key to search for
     * @return true if the key is found, false otherwise
     */
    public boolean sequentialSearch(T key) {
        if (root == null) {
            System.out.println("Tree is empty.");
            return false;  // Tree is empty
        }
        return recursiveSearch(root, key);
    }

    /**
     * Recursive helper function for sequential search in the B+ Tree.
     *
     * @param currentNode the current node to search within
     * @param key the key to search for
     * @return true if the key is found, false otherwise
     */
    private boolean recursiveSearch(Node<T> currentNode, T key) {
        // Nếu là leaf node, kiểm tra nếu khóa có trong node này
        if (currentNode.isLeaf()) {
            System.out.println("Checking leaf node: " + currentNode.getKeys());
            return currentNode.getKeys().contains(key);
        }

        // In thông tin node nội bộ
        System.out.println("Current internal node keys: " + currentNode.getKeys());

        // Sử dụng `findChildIndex` để tìm vị trí của node con
        int childIndex = findChildIndex(currentNode, key);

        // Kiểm tra giới hạn của chỉ số con
        if (childIndex >= currentNode.getChildren().size()) {
            System.out.println("Out of bounds: No child at index " + childIndex);
            return false;
        }

        System.out.println("Moving to child node at index " + childIndex);
        return recursiveSearch(currentNode.getChildren().get(childIndex), key);
    }

    /**
     * Finds the appropriate child index for a given key within an internal node using binary search.
     *
     * @param node the internal node
     * @param key the key to locate
     * @return the index of the child node to follow
     */
    private int findChildIndex(Node<T> node, T key) {
        List<T> keys = node.getKeys();
        int pos = Collections.binarySearch(keys, key);

        // Nếu khóa được tìm thấy trong keys, đi đến child node bên trái của vị trí pos
        if (pos >= 0) {
            return pos;
        } else {
            // Nếu không tìm thấy, chuyển thành vị trí chèn thích hợp
            int insertionPoint = -pos - 1;

            // insertionPoint xác định `child node` có giá trị lớn hơn hoặc bằng key
            return insertionPoint;
        }
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
     * Prints the structure of the B+ Tree.
     */
    public void printTreeStructure() {
        if (root == null) {
            System.out.println("The tree is empty.");
        } else {
            System.out.println("B+ Tree Structure:");
            printNode(root, 0, null);
        }
    }

    /**
     * Recursively prints the given node and its children with indentation, including links to child nodes.
     *
     * @param node  the node to print
     * @param level the current level in the tree (used for indentation)
     * @param parent the parent node if applicable (used for indicating relationships)
     */
    private void printNode(Node<T> node, int level, Node<T> parent) {
        StringBuilder indent = new StringBuilder();
        for (int i = 0; i < level; i++) {
            indent.append("  ");
        }

        // Print node type, keys, and parent info if applicable
        System.out.println(indent + (node.isLeaf() ? "Leaf " : "Internal ") +
                "Node " + node.getKeys() +
                (parent != null ? " (Parent Keys: " + parent.getKeys() + ")" : ""));

        if (!node.isLeaf()) {
            System.out.print(indent + "  Children Keys: ");
            for (Node<T> child : node.getChildren()) {
                System.out.print(child.getKeys() + " ");
            }
            System.out.println();

            // Recursively print each child
            for (Node<T> child : node.getChildren()) {
                printNode(child, level + 1, node);
            }
        } else {
            // Print link to the next leaf node if it exists
            if (node.getNext() != null) {
                System.out.println(indent + "  --> Linked to next Leaf Node with keys: " + node.getNext().getKeys());
            }
        }
    }

    /**
     * Traverses through the leaf nodes and prints all keys in ascending order.
     */
    public void traverseLeaves() {
        if (root == null) {
            System.out.println("The tree is empty.");
            return;
        }

        // Navigate to the leftmost leaf node
        Node<T> currentNode = root;
        while (!currentNode.isLeaf()) {
            currentNode = currentNode.getChildren().get(0);  // Go to the first child in each internal node
        }

        // Traverse through all leaf nodes and print keys
        System.out.print("Leaf nodes in ascending order: ");
        while (currentNode != null) {
            System.out.print(currentNode.getKeys() + " ");
            currentNode = currentNode.getNext();  // Move to the next leaf node
        }
        System.out.println();
    }

}
