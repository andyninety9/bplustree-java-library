package org.bptree;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class BPlusTreeTest {

    @Test
    public void testBottomUpTreeStructure() {
        BPlusTree<Integer> bPlusTree = new BPlusTree<>(3);
        List<Integer> keys = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);

        try {
            bPlusTree.bottom_up_method(keys);
        } catch (InterruptedException | ExecutionException e) {
            fail("Exception occurred during tree construction: " + e.getMessage());
        }

        // Verify tree structure
        Node<Integer> root = bPlusTree.getRoot();
        assertNotNull(root, "The root should not be null.");
        assertFalse(root.isLeaf(), "The root should be an internal node.");

        // Print tree structure for debugging purposes
        bPlusTree.printTreeStructure();

        // Verify the leaf nodes contain the correct keys
        Node<Integer> current = root;
        while (!current.isLeaf()) {
            current = current.getChildren().get(0);
        }

        int expectedKey = 1;
        while (current != null) {
            for (Integer key : current.getKeys()) {
                assertEquals(expectedKey, key, "The leaf node should contain the key " + expectedKey);
                expectedKey++;
            }
            current = current.getNext();
        }

        assertEquals(10, expectedKey, "The keys should go from 1 to 9.");
    }

    @Test
    public void testTreeHeight() {
        BPlusTree<Integer> bPlusTree = new BPlusTree<>(3);
        List<Integer> keys = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);

        try {
            bPlusTree.bottom_up_method(keys);
        } catch (InterruptedException | ExecutionException e) {
            fail("Exception occurred during tree construction: " + e.getMessage());
        }

        int height = bPlusTree.getHeight();
        assertEquals(3, height, "The height of the tree should be 3.");
    }

    @Test
    public void testEmptyTree() {
        BPlusTree<Integer> bPlusTree = new BPlusTree<>(3);
        assertEquals(0, bPlusTree.getHeight(), "The height of an empty tree should be 0.");
        assertNull(bPlusTree.getRoot(), "The root of an empty tree should be null.");
    }

    @Test
    public void testDuplicateKeys() {
        BPlusTree<Integer> bPlusTree = new BPlusTree<>(3);
        List<Integer> keys = Arrays.asList(1, 2, 2, 3, 4, 5);

        try {
            bPlusTree.bottom_up_method(keys);
        } catch (InterruptedException | ExecutionException e) {
            fail("Exception occurred during tree construction: " + e.getMessage());
        }

        // Print tree structure for debugging purposes
        bPlusTree.printTreeStructure();

        // Verify the leaf nodes contain the correct keys, including duplicates
        Node<Integer> current = bPlusTree.getRoot();
        while (!current.isLeaf()) {
            current = current.getChildren().get(0);
        }

        List<Integer> expectedKeys = Arrays.asList(1, 2, 2, 3, 4, 5);
        for (Integer expectedKey : expectedKeys) {
            boolean found = false;
            Node<Integer> leaf = current;

            while (leaf != null) {
                if (leaf.getKeys().contains(expectedKey)) {
                    found = true;
                    break;
                }
                leaf = leaf.getNext();
            }

            assertTrue(found, "The leaf node should contain the key " + expectedKey);
        }
    }


    @Test
    public void testLargeNumberOfKeys() {
        BPlusTree<Integer> bPlusTree = new BPlusTree<>(4);
        int numKeys = 100;
        List<Integer> keys = new java.util.ArrayList<>();
        for (int i = 1; i <= numKeys; i++) {
            keys.add(i);
        }

        try {
            bPlusTree.bottom_up_method(keys);
        } catch (InterruptedException | ExecutionException e) {
            fail("Exception occurred during tree construction: " + e.getMessage());
        }

        // Verify tree structure
        Node<Integer> root = bPlusTree.getRoot();
        assertNotNull(root, "The root should not be null.");
        assertFalse(root.isLeaf(), "The root should be an internal node.");

        // Verify the leaf nodes contain the correct keys
        Node<Integer> current = root;
        while (!current.isLeaf()) {
            current = current.getChildren().get(0);
        }

        int expectedKey = 1;
        while (current != null) {
            for (Integer key : current.getKeys()) {
                assertEquals(expectedKey, key, "The leaf node should contain the key " + expectedKey);
                expectedKey++;
            }
            current = current.getNext();
        }

        assertEquals(numKeys + 1, expectedKey, "The keys should go from 1 to " + numKeys);
    }
}
