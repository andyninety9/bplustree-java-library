package org.bptree.utils;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SortUtilsTest {

    @Test
    public void testMergeSort() {
        List<Integer> data = new ArrayList<>(List.of(5, 2, 9, 1, 5, 6));
        List<Integer> sortedData = SortUtils.mergeSort(data);
        assertEquals(List.of(1, 2, 5, 5, 6, 9), sortedData, "The list should be sorted in ascending order.");
    }
}