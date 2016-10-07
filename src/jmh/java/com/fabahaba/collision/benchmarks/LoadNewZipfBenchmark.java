package com.fabahaba.collision.benchmarks;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.fabahaba.collision.benchmarks.LoadStaticZipfBenchmark.ITEMS;
import static com.fabahaba.collision.benchmarks.LoadStaticZipfBenchmark.MASK;
import static com.fabahaba.collision.benchmarks.LoadStaticZipfBenchmark.SIZE;

@State(Scope.Benchmark)
@Threads(32)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 10)
@Measurement(iterations = 20)
public class LoadNewZipfBenchmark {

  @Param({
             //"Cache2k",
             "Caffeine",
             "Collision",
             "Collision_Load_Atomic"
         })
  LoadStaticZipfBenchmark.BenchmarkFunctionFactory cacheType;
  Function<Long, Long> benchmarkFunction;
  final Long[] keys = new Long[SIZE];
  final ScrambledZipfGenerator generator = new ScrambledZipfGenerator(ITEMS);

  @Setup(Level.Iteration)
  public void setup() {
    if (benchmarkFunction == null) {
      this.benchmarkFunction = cacheType.create();
    }
    IntStream.range(0, keys.length).parallel().forEach(i -> keys[i] = generator.nextValue());
  }

  @Benchmark
  public Long spread(final LoadStaticZipfBenchmark.ThreadState threadState) {
    return benchmarkFunction.apply(keys[threadState.index++ & MASK]);
  }
}
