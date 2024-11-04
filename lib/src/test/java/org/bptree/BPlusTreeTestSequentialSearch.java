package org.bptree;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class BPlusTreeTestSequentialSearch {

    private static final int TREE_ORDER = 4;
    private static BPlusTree<Integer> bPlusTree;

    @BeforeAll
    public static void setUp() throws InterruptedException, ExecutionException {
        bPlusTree = new BPlusTree<>(TREE_ORDER);
        List<Integer> data = new ArrayList<>();
        for (int i = 0; i < 21; i++) {
            data.add(i);
        }
        bPlusTree.bottom_up_method(data);
    }

    @Test
    public void testBasicSequentialSearch() {

        System.out.println("-================-TRAVERSAL-================-");
        bPlusTree.traverseLeaves();

        System.out.println("-================-HEIGHT-================-");
        System.out.println(bPlusTree.getHeight());

        System.out.println("-================-SEARCH-================-");
        int key = 9;
        boolean isFound = bPlusTree.sequentialSearch(key);
        if (isFound) {
            System.out.println("Found " + key);
        }else{
            System.out.println("Not found " + key);
        }

    }

}
