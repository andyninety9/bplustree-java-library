package org.bptree;

import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

public class BPlusTreeTest {

    @Test
    public void testBottomUpTreeStructure() throws Exception {
        // Construct the B+ Tree with order 4 (this means each node can have at most 3 keys)
        int order = 4;
        BPlusTree<Integer> bPlusTree = new BPlusTree<>(order);

        // Insert a list of keys to create the tree (ensuring keys are sorted)
        List<Integer> keys = new ArrayList<>();
        for (int i = 1; i <= 20; i++) {
            keys.add(i);
        }
        keys.sort(Integer::compareTo);  // Ensure the keys are sorted before building the tree
        bPlusTree.bottom_up_method(keys);

        // Get the root of the tree
        Node<Integer> root = bPlusTree.getRoot();
        assertNotNull(root, "The root should not be null after constructing the tree.");

        // Traverse from root to leaf to ensure correctness of internal node connections
        for (Integer key : keys) {
            Node<Integer> leafNode = findLeafNode(root, key);
            assertNotNull(leafNode, "The leaf node for key " + key + " should not be null.");
            assertTrue(leafNode.getKeys().contains(key), "The leaf node should contain the key " + key + ".");
        }

        // Ensure that leaf nodes are linked in sequence correctly
        Node<Integer> currentLeaf = findLeftMostLeaf(root);
        int expectedKey = 1;
        while (currentLeaf != null) {
            List<Integer> leafKeys = currentLeaf.getKeys();
            System.out.println("Leaf node keys: " + leafKeys);
            for (Integer key : leafKeys) {
                assertEquals(expectedKey, key, "Keys in leaf nodes should be in sequential order.");
                expectedKey++;
            }
            currentLeaf = currentLeaf.getNext();
        }

        // Ensure all keys are visited
        assertEquals(21, expectedKey, "All keys from 1 to 20 should be present in the leaf nodes.");

        // Additional check to verify that internal nodes have the correct children and keys
        verifyInternalNodeStructure(root, order);
    }

    /**
     * Finds the leaf node that should contain the given key by traversing the tree from the root.
     *
     * @param current the current node being traversed
     * @param key     the key to find
     * @return the leaf node that contains the key, or null if not found
     */
    private Node<Integer> findLeafNode(Node<Integer> current, Integer key) {
        while (current != null && !current.isLeaf()) {
            // Traverse internal nodes to find the correct child node
            List<Integer> keys = current.getKeys();
            List<Node<Integer>> children = current.getChildren();
            int i = 0;
            while (i < keys.size() && key > keys.get(i)) {  // Locate the correct child node
                i++;
            }
            current = children.get(i);
        }
        return current;
    }

    /**
     * Finds the leftmost leaf node in the tree by traversing from the root.
     *
     * @param root the root node of the tree
     * @return the leftmost leaf node
     */
    private Node<Integer> findLeftMostLeaf(Node<Integer> root) {
        Node<Integer> current = root;
        while (current != null && !current.isLeaf()) {
            current = current.getChildren().get(0);
        }
        return current;
    }

    /**
     * Verifies the structure of internal nodes to ensure each internal node has the correct number of children and keys.
     *
     * @param node  the current internal node being verified
     * @param order the order of the B+ Tree
     */
    private void verifyInternalNodeStructure(Node<Integer> node, int order) {
        if (node.isLeaf()) {
            return;
        }

        List<Node<Integer>> children = node.getChildren();
        List<Integer> keys = node.getKeys();

        // Internal node should have at most 'order' children
        assertTrue(children.size() <= order, "Internal node should have at most " + order + " children.");
        // Number of keys should be one less than the number of children
        assertEquals(children.size() - 1, keys.size(), "The number of keys in an internal node should be one less than the number of children.");

        for (Node<Integer> child : children) {
            verifyInternalNodeStructure(child, order);
        }
    }
}
