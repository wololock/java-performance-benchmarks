package com.github.wololock;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public final class Partition<T> extends AbstractList<List<T>> {

    private final List<T> list;
    private final int chunkSize;

    public Partition(List<T> list, int chunkSize) {
        this.list = new ArrayList<>(list);
        this.chunkSize = chunkSize;
    }

    public static <T> Partition<T> ofSize(List<T> list, int chunkSize) {
        return new Partition<>(list, chunkSize);
    }

    public static void main(String[] args) {
        final List<Integer> numbers = Arrays.asList(1,2,3,4,5,6,7);
        final int chunkSize = 3;

//        System.out.println(Partition.ofSize(numbers, 3));
//        System.out.println(Partition.ofSize(numbers, 2));

//        final List<List<Integer>> result = new ArrayList<>();
//        final AtomicInteger counter = new AtomicInteger();
//
//
//        for (int number : numbers) {
//            if (counter.getAndIncrement() % chunkSize == 0) {
//                result.add(new ArrayList<>());
//            }
//            result.get(result.size() - 1).add(number);
//        }

        final AtomicInteger counter = new AtomicInteger();

        final Collection<List<Integer>> result = numbers.stream()
            .collect(Collectors.groupingBy(it -> counter.getAndIncrement() / chunkSize))
            .values();

        System.out.println(result);
    }

    @Override
    public List<T> get(int index) {
        int start = index * chunkSize;
        int end = Math.min(start + chunkSize, list.size());

        if (start > end) {
            throw new IndexOutOfBoundsException("Index " + index + " is out of the list range <0," + (size() - 1) + ">");
        }

        return new ArrayList<>(list.subList(start, end));
    }

    @Override
    public int size() {
        return (int) Math.ceil((double) list.size() / (double) chunkSize);
    }
}
