package com.example.spark;

public class Main {
    public static void main(String[] args) {
        System.out.println("Hello world");


        decrement(5);
    }

    private static void generateStackOverFlow() {
        generateStackOverFlow();
    }

    private static void decrement(int x) {
        if (x >= 0) {
            decrement(--x);
            System.out.println(++x);
        }
    }
}
