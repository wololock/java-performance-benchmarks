package bench;

import com.github.wololock.Partition;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Java6Assertions.assertThat;

@State(Scope.Benchmark)
public class JavaListPartitionBenchmark {

    private static final int CHUNK_SIZE_SMALL = 3;
    private static final int SMALL_LIST = 20;

    private static final int CHUNK_SIZE_LARGE = 23;
    private static final int LARGE_LIST = 10_000;

    private static final int CHUNK_SIZE_HUGE = 1024;
    private static final int HUGE_LIST = 10_000_000;

    private static final List<Integer> smallList = IntStream.range(0, SMALL_LIST).boxed().collect(toList());
    private static final List<Integer> expectedSmallChunk = Arrays.asList(6,7,8);
    private static final int expectedSizeSmall = 7;

    private static final List<Integer> largeList = IntStream.range(0, LARGE_LIST).boxed().collect(toList());
    private static final List<Integer> expectedLargeChunk = IntStream.rangeClosed(46,68).boxed().collect(toList());
    private static final int expectedSizeLarge = 435;

    private static final List<Integer> hugeList = IntStream.range(0, HUGE_LIST).boxed().collect(toList());
    private static final List<Integer> expectedHugeChunk = IntStream.rangeClosed(2048,3071).boxed().collect(toList());
    private static final int expectedSizeHuge = 9766;

    @Benchmark
    public void smallListImperative() {
        //given:
        final List<List<Integer>> result = new ArrayList<>();
        final AtomicInteger counter = new AtomicInteger();

        //when:
        for (int number : smallList) {
            if (counter.getAndIncrement() % CHUNK_SIZE_SMALL == 0) {
                result.add(new ArrayList<>());
            }
            result.get(result.size() - 1).add(number);
        }

        //then:
        assertThat(result).hasSize(expectedSizeSmall);
        //and:
        assertThat(result.get(2)).isEqualTo(expectedSmallChunk);
    }

    @Benchmark
    public void smallListStreamGroupingBy() {
        //given:
        final AtomicInteger counter = new AtomicInteger();

        //when:
        final List<List<Integer>> result = new ArrayList<>(smallList.stream()
            .collect(Collectors.groupingBy(it -> counter.getAndIncrement() / CHUNK_SIZE_SMALL))
            .values());

        //then:
        assertThat(result).hasSize(expectedSizeSmall);
        //and:
        assertThat(result.get(2)).isEqualTo(expectedSmallChunk);
    }

    @Benchmark
    public void smallListStreamPartitioned() {
        //when:
        final List<List<Integer>> result = smallList.stream()
            .collect(partitioned(CHUNK_SIZE_SMALL));

        //then:
        assertThat(result).hasSize(expectedSizeSmall);
        //and:
        assertThat(result.get(2)).isEqualTo(expectedSmallChunk);
    }

    @Benchmark
    public void smallListToPartition() {
        //when:
        final List<List<Integer>> result = Partition.ofSize(smallList, CHUNK_SIZE_SMALL);

        //then:
        assertThat(result).hasSize(expectedSizeSmall);
        //and:
        assertThat(result.get(2)).isEqualTo(expectedSmallChunk);
    }

    @Benchmark
    public void largeListImperative() {
        //given:
        final List<List<Integer>> result = new ArrayList<>();
        final AtomicInteger counter = new AtomicInteger();

        //when:
        for (int number : largeList) {
            if (counter.getAndIncrement() % CHUNK_SIZE_LARGE == 0) {
                result.add(new ArrayList<>());
            }
            result.get(result.size() - 1).add(number);
        }

        //then:
        assertThat(result).hasSize(expectedSizeLarge);
        //and:
        assertThat(result.get(2)).isEqualTo(expectedLargeChunk);
    }

    @Benchmark
    public void largeListStreamGroupingBy() {
        //given:
        final AtomicInteger counter = new AtomicInteger();

        //when:
        final List<List<Integer>> result = new ArrayList<>(largeList.stream()
            .collect(Collectors.groupingBy(it -> counter.getAndIncrement() / CHUNK_SIZE_LARGE))
            .values());

        //then:
        assertThat(result).hasSize(expectedSizeLarge);
        //and:
        assertThat(result.get(2)).isEqualTo(expectedLargeChunk);
    }

    @Benchmark
    public void largeListStreamPartitioned() {
        //when:
        final List<List<Integer>> result = largeList.stream()
            .collect(partitioned(CHUNK_SIZE_LARGE));

        //then:
        assertThat(result).hasSize(expectedSizeLarge);
        //and:
        assertThat(result.get(2)).isEqualTo(expectedLargeChunk);
    }

    @Benchmark
    public void largeListToPartition() {
        //when:
        final List<List<Integer>> result = Partition.ofSize(largeList, CHUNK_SIZE_LARGE);

        //then:
        assertThat(result).hasSize(expectedSizeLarge);
        //and:
        assertThat(result.get(2)).isEqualTo(expectedLargeChunk);
    }

    @Benchmark
    public void hugeListImperative() {
        //given:
        final List<List<Integer>> result = new ArrayList<>();
        final AtomicInteger counter = new AtomicInteger();

        //when:
        for (int number : hugeList) {
            if (counter.getAndIncrement() % CHUNK_SIZE_HUGE == 0) {
                result.add(new ArrayList<>());
            }
            result.get(result.size() - 1).add(number);
        }

        //then:
        assertThat(result).hasSize(expectedSizeHuge);
        //and:
        assertThat(result.get(2)).isEqualTo(expectedHugeChunk);
    }

    @Benchmark
    public void hugeListStreamGroupingBy() {
        //given:
        final AtomicInteger counter = new AtomicInteger();

        //when:
        final List<List<Integer>> result = new ArrayList<>(hugeList.stream()
            .collect(Collectors.groupingBy(it -> counter.getAndIncrement() / CHUNK_SIZE_HUGE))
            .values());

        //then:
        assertThat(result).hasSize(expectedSizeHuge);
        //and:
        assertThat(result.get(2)).isEqualTo(expectedHugeChunk);
    }

    @Benchmark
    public void hugeListStreamPartitioned() {
        //when:
        final List<List<Integer>> result = hugeList.stream()
            .collect(partitioned(CHUNK_SIZE_HUGE));

        //then:
        assertThat(result).hasSize(expectedSizeHuge);
        //and:
        assertThat(result.get(2)).isEqualTo(expectedHugeChunk);
    }

    @Benchmark
    public void hugeListToPartition() {
        //when:
        final List<List<Integer>> result = Partition.ofSize(hugeList, CHUNK_SIZE_HUGE);

        //then:
        assertThat(result).hasSize(expectedSizeHuge);
        //and:
        assertThat(result.get(2)).isEqualTo(expectedHugeChunk);
    }


    private static <T> Collector<T, List<T>, List<List<T>>> partitioned(int chunkSize) {
        return Collector.of(
            ArrayList::new,
            List::add,
            (a,b) -> { a.addAll(b); return a; },
            a -> Partition.ofSize(a, chunkSize),
            Collector.Characteristics.UNORDERED
        );
    }

}
