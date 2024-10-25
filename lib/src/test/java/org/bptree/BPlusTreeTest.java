package org.bptree;

import org.junit.jupiter.api.Test;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the BPlusTree class.
 */
public class BPlusTreeTest {

    @Test
    public void testTreeHeightWithIntegers() throws Exception {
        BPlusTree<Integer> tree = new BPlusTree<>(4);
        tree.bottom_up_method(List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        assertEquals(3, tree.getHeight(), "The tree height should be 3.");
    }

    @Test
    public void testTreeHeightWithStrings() throws Exception {
        BPlusTree<String> tree = new BPlusTree<>(4);
        tree.bottom_up_method(List.of("Apple", "Banana", "Cherry", "Date"));
        assertEquals(2, tree.getHeight(), "The tree height should be 2.");
    }

    @Test
    public void testLeafNodeLinks() throws Exception {
        BPlusTree<Integer> tree = new BPlusTree<>(4);
        tree.bottom_up_method(List.of(1, 2, 3, 4, 5, 6));  // Đảm bảo đủ dữ liệu để tạo nhiều leaf nodes

        Node<Integer> firstLeaf = tree.getRoot();

        // Traverse down to the first leaf node if the root is an internal node
        while (!firstLeaf.isLeaf()) {
            firstLeaf = firstLeaf.getChildren().get(0);
        }

        // Ensure the first leaf node has a next link
        assertNotNull(firstLeaf.getNext(), "The first leaf node should have a next link.");
        assertEquals(4, firstLeaf.getNext().getKeys().get(0),
                "The next leaf node should contain the key 4.");
    }

    @Test
    public void testInvalidOrderThrowsException() {
        Exception exception = assertThrows(IllegalArgumentException.class,
                () -> new BPlusTree<>(2));
        assertEquals("Order must be at least 3.", exception.getMessage());
    }
}
