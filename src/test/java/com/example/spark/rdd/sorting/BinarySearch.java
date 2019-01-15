package com.example.spark.rdd.sorting;

import org.apache.commons.lang3.builder.ToStringExclude;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class BinarySearch {
    @Test
    public void binarySearch() {
        int arraySize = 128;
        int[] sortedArray = new int[arraySize];
        for (int i = 0; i < arraySize; i++) {
            sortedArray[i] = i;
        }

        int toFind = 126;

        Integer integer = searchWhile(toFind, sortedArray);
        System.out.println("Result index: " + integer);
    }

    private Integer searchWhile(int toFind, int[] where) {
        int count = 0;

        int begin = 0;
        int end = where.length - 1;

        while (begin <= end) {
            System.out.println("Attempt number: " + ++count);

            int mid = (begin + end) / 2;
            int guess = where[mid];

            if (guess == toFind) {
                return mid;
            }

            if (guess > toFind) {
                end = mid - 1;
            } else {
                begin = mid + 1;
            }
        }

        return null;
    }

    @Test
    public void searchBench() {
        int numberOfElems = 100_000_000;

        int[] sortedArray = new int[numberOfElems];

        for (int i = 0; i < numberOfElems; i++) {
            sortedArray[i] = i;
        }

        int toFind = 99_999_999;

        long begin = System.currentTimeMillis();
        for (int i = 0; i < sortedArray.length; i++) {
            if (toFind == sortedArray[i]) {
                System.out.println("--Simple search");
                System.out.println(System.currentTimeMillis() - begin);
            }
        }

        begin = System.currentTimeMillis();
        searchWhile(toFind, sortedArray);
        System.out.println("--Binary search");
        System.out.println(System.currentTimeMillis() - begin);
    }
}
