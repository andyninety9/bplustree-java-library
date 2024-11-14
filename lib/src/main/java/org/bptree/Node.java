package org.bptree;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Represents a node in the B+ Tree.
 * Each node can either be a leaf node or an internal node.
 * Internal nodes store child pointers, while leaf nodes store links to other leaves.
 *
 * @param <T> the type of keys stored in this node
 * @author andymai
 * @version 1.0
 */
public class Node<T> implements Serializable {
    private final boolean isLeaf;  // True for leaf nodes, False for internal nodes
    private final List<T> keys;  // List of keys stored in this node
    private final List<Node<T>> children;  // List of child nodes (only for internal nodes)
    private Node<T> next;  // Link to the next leaf node (for leaf nodes)

    /**
     * Constructs a new Node.
     *
     * @param isLeaf whether the node is a leaf node
     */
    public Node(boolean isLeaf) {
        this.isLeaf = isLeaf;
        this.keys = new ArrayList<>();
        this.children = isLeaf ? Collections.emptyList() : new ArrayList<>();
        this.next = null;
    }

    /**
     * Checks if this node is a leaf node.
     *
     * @return true if this is a leaf node, false otherwise
     */
    public boolean isLeaf() {
        return isLeaf;
    }

    /**
     * Returns the list of keys stored in this node.
     *
     * @return the list of keys
     */
    public List<T> getKeys() {
        return keys;
    }

    /**
     * Returns the list of child nodes.
     * This is only applicable for internal nodes.
     *
     * @return the list of child nodes, or an empty list if this is a leaf node
     */
    public List<Node<T>> getChildren() {
        return children;
    }

    /**
     * Sets the link to the next leaf node.
     *
     * @param next the next leaf node
     */
    public void setNext(Node<T> next) {
        this.next = next;
    }

    /**
     * Returns the next leaf node linked to this node.
     *
     * @return the next leaf node, or null if there is no next node
     */
    public Node<T> getNext() {
        return next;
    }

    /**
     * Adds a key to the current node.
     *
     * @param key the key to be added
     */
    public void addKey(T key) {
        keys.add(key);
    }

    /**
     * Adds a child node to this node.
     * This method should only be used for internal nodes.
     *
     * @param child the child node to be added
     * @throws UnsupportedOperationException if this is a leaf node
     */
    public void addChild(Node<T> child) {
        if (isLeaf) {
            throw new UnsupportedOperationException("Cannot add children to a leaf node.");
        }
        children.add(child);
    }

    @Override
    public String toString() {
        return "Node{" +
                "isLeaf=" + isLeaf +
                ", keys=" + keys +
                '}';
    }
}
