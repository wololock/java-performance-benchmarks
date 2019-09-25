package bench;

import com.github.wololock.Partition;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Java6Assertions.assertThat;

public class JavaListPartitionBenchmark {

    private static final Random random = new Random();

    @State(Scope.Benchmark)
    static public class A_SmallList {
        private static final int SMALL_LIST_SIZE = 20;

        private static final List<Integer> list = IntStream.range(0, SMALL_LIST_SIZE + random.nextInt(13)).boxed().collect(toList());

        private int chunkSize;
        private List<Integer> expectedChunk;
        private int chunkIndex;
        private int partitionedSize;

        @Setup(Level.Iteration)
        public void setup() {
            chunkSize = random.nextInt(3) + 2;
            partitionedSize = (int) Math.ceil((double) list.size() / chunkSize);
            chunkIndex = random.nextInt(partitionedSize - 1);
            expectedChunk = list.subList(chunkSize * chunkIndex, chunkSize * (chunkIndex + 1));
        }

        @Benchmark
        public void A1_smallListImperative() {
            //given:
            final List<List<Integer>> result = new ArrayList<>();
            final AtomicInteger counter = new AtomicInteger();

            //when:
            for (int number : list) {
                if (counter.getAndIncrement() % chunkSize == 0) {
                    result.add(new ArrayList<>());
                }
                result.get(result.size() - 1).add(number);
            }

            //then:
            assertThat(result).hasSize(partitionedSize);
            //and:
            assertThat(result.get(chunkIndex)).isEqualTo(expectedChunk);
        }

        @Benchmark
        public void A2_smallListStreamGroupingBy() {
            //given:
            final AtomicInteger counter = new AtomicInteger();

            //when:
            final List<List<Integer>> result = new ArrayList<>(list.stream()
                .collect(Collectors.groupingBy(it -> counter.getAndIncrement() / chunkSize))
                .values());

            //then:
            assertThat(result).hasSize(partitionedSize);
            //and:
            assertThat(result.get(chunkIndex)).isEqualTo(expectedChunk);
        }

        @Benchmark
        public void A3_smallListStreamPartitioned() {
            //when:
            final List<List<Integer>> result = list.stream()
                .collect(partitioned(chunkSize));

            //then:
            assertThat(result).hasSize(partitionedSize);
            //and:
            assertThat(result.get(chunkIndex)).isEqualTo(expectedChunk);
        }

        @Benchmark
        public void A4_smallListToPartition() {
            //when:
            final List<List<Integer>> result = Partition.ofSize(list, chunkSize);

            //then:
            assertThat(result).hasSize(partitionedSize);
            //and:
            assertThat(result.get(chunkIndex)).isEqualTo(expectedChunk);
        }
    }

    @State(Scope.Benchmark)
    static public class B_LargeList {

        private static final int LARGE_LIST_SIZE = 10_000;

        private static final List<Integer> list = IntStream.range(0, LARGE_LIST_SIZE + random.nextInt(199)).boxed().collect(toList());

        private int chunkSize;
        private List<Integer> expectedChunk;
        private int chunkIndex;
        private int expectedSize;

        @Setup(Level.Iteration)
        public void setup() {
            chunkSize = random.nextInt(5) + 20;
            expectedSize = (int) Math.ceil((double) list.size() / chunkSize);
            chunkIndex = random.nextInt(expectedSize - 1);
            expectedChunk = list.subList(chunkSize * chunkIndex, chunkSize * (chunkIndex + 1));
        }

        @Benchmark
        public void B1_largeListImperative() {
            //given:
            final List<List<Integer>> result = new ArrayList<>();
            final AtomicInteger counter = new AtomicInteger();

            //when:
            for (int number : list) {
                if (counter.getAndIncrement() % chunkSize == 0) {
                    result.add(new ArrayList<>());
                }
                result.get(result.size() - 1).add(number);
            }

            //then:
            assertThat(result).hasSize(expectedSize);
            //and:
            assertThat(result.get(chunkIndex)).isEqualTo(expectedChunk);
        }

        @Benchmark
        public void B2_largeListStreamGroupingBy() {
            //given:
            final AtomicInteger counter = new AtomicInteger();

            //when:
            final List<List<Integer>> result = new ArrayList<>(list.stream()
                .collect(Collectors.groupingBy(it -> counter.getAndIncrement() / chunkSize))
                .values());

            //then:
            assertThat(result).hasSize(expectedSize);
            //and:
            assertThat(result.get(chunkIndex)).isEqualTo(expectedChunk);
        }

        @Benchmark
        public void B3_largeListStreamPartitioned() {
            //when:
            final List<List<Integer>> result = list.stream()
                .collect(partitioned(chunkSize));

            //then:
            assertThat(result).hasSize(expectedSize);
            //and:
            assertThat(result.get(chunkIndex)).isEqualTo(expectedChunk);
        }

        @Benchmark
        public void B4_largeListToPartition() {
            //when:
            final List<List<Integer>> result = Partition.ofSize(list, chunkSize);

            //then:
            assertThat(result).hasSize(expectedSize);
            //and:
            assertThat(result.get(chunkIndex)).isEqualTo(expectedChunk);
        }
    }

    @State(Scope.Benchmark)
    static public class C_HugeList {

        private static final int HUGE_LIST_SIZE = 10_000_000;

        private static final List<Integer> list = IntStream.range(0, HUGE_LIST_SIZE + random.nextInt(100_000)).boxed().collect(toList());

        private int chunkSize;
        private List<Integer> expectedChunk;
        private int chunkIndex;
        private int expectedSize;

        @Setup(Level.Iteration)
        public void setup() {
            chunkSize = random.nextInt(16) + 1024;
            expectedSize = (int) Math.ceil((double) list.size() / chunkSize);
            chunkIndex = random.nextInt(expectedSize - 1);
            expectedChunk = list.subList(chunkSize * chunkIndex, chunkSize * (chunkIndex + 1));
        }

        @Benchmark
        public void C1_hugeListImperative() {
            //given:
            final List<List<Integer>> result = new ArrayList<>();
            final AtomicInteger counter = new AtomicInteger();

            //when:
            for (int number : list) {
                if (counter.getAndIncrement() % chunkSize == 0) {
                    result.add(new ArrayList<>());
                }
                result.get(result.size() - 1).add(number);
            }

            //then:
            assertThat(result).hasSize(expectedSize);
            //and:
            assertThat(result.get(chunkIndex)).isEqualTo(expectedChunk);
        }

        @Benchmark
        public void C2_hugeListStreamGroupingBy() {
            //given:
            final AtomicInteger counter = new AtomicInteger();

            //when:
            final List<List<Integer>> result = new ArrayList<>(list.stream()
                .collect(Collectors.groupingBy(it -> counter.getAndIncrement() / chunkSize))
                .values());

            //then:
            assertThat(result).hasSize(expectedSize);
            //and:
            assertThat(result.get(chunkIndex)).isEqualTo(expectedChunk);
        }

        @Benchmark
        public void C3_hugeListStreamPartitioned() {
            //when:
            final List<List<Integer>> result = list.stream()
                .collect(partitioned(chunkSize));

            //then:
            assertThat(result).hasSize(expectedSize);
            //and:
            assertThat(result.get(chunkIndex)).isEqualTo(expectedChunk);
        }

        @Benchmark
        public void C4_hugeListToPartition() {
            //when:
            final List<List<Integer>> result = Partition.ofSize(list, chunkSize);

            //then:
            assertThat(result).hasSize(expectedSize);
            //and:
            assertThat(result.get(chunkIndex)).isEqualTo(expectedChunk);
        }
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
