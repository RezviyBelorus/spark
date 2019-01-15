package com.example.spark.rdd.sorting;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

public class SortingTest {
    @Test
    public void bubbleTest() {
        List<Integer> actual = Arrays.asList(1, 7, 3, 4, 2);

        System.out.println("--Before sort--");
        System.out.println(actual);

        for (int i = 0; i < actual.size(); i++) {
            boolean isSorted = true;

            for (int j = 1; j < actual.size() - i; j++) {
                Integer current = actual.get(j - 1);
                Integer next = actual.get(j);

                if (current > next) {
                    actual.set(j, current);
                    actual.set(j - 1, next);
                    isSorted = false;
                }
            }

            if (isSorted) {
                break;
            }
        }
        System.out.println("--After sort--");
        System.out.println(actual);

        List<Integer> expected = Arrays.asList(1, 2, 3, 4, 7);
        assertEquals(expected, actual);
    }

    @Test
    public void bubbleSort() {
        Integer[] integers = new Integer[]{3, 4, 2, 1};

        System.out.println("--Before--");
        for (Integer integer : integers) {
            System.out.print(integer + " ");
        }

        int n = integers.length;

        IntStream.range(0, n - 1)
                .flatMap(i -> IntStream.range(1, n - i))
                .forEach(j -> {
                    if (integers[j - 1] > integers[j]) {
                        int temp = integers[j];
                        integers[j] = integers[j - 1];
                        integers[j - 1] = temp;
                    }
                });

        System.out.println("--After");
        System.out.println(integers);
        for (Integer integer : integers) {
            System.out.print(integer + " ");
        }
    }

    @Test
    public void intStream() {
        IntStream.range(0, 10)
                .flatMap(i -> IntStream.range(1, 9))
                .forEach(i -> System.out.println(i));
    }

    @Test
    public void flatMap() {
        List<List<Integer>> lists = Arrays.asList(Arrays.asList(1, 2, 3, 4),
                Arrays.asList(5, 6, 7, 8));

        lists.stream()
                .flatMap(integers -> integers.stream())
                .forEach(integer -> System.out.println(integer));


    }
}
