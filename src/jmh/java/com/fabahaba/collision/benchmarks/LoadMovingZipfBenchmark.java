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
@Warmup(iterations = 1)
@Measurement(iterations = 10)
public class LoadMovingZipfBenchmark {

  @Param({
             "Cache2k",
             "Caffeine",
             "Collision",
             "Collision_Aggressive"
         })
  private LoadStaticZipfBenchmark.BenchmarkFunctionFactory cacheType;
  private Function<Long, Long> benchmarkFunction;
  private final Long[] keys = new Long[SIZE];
  private final ScrambledZipfGenerator generator = new ScrambledZipfGenerator(ITEMS);

  @Setup(Level.Iteration)
  public void setup() {
    if (benchmarkFunction == null) {
      this.benchmarkFunction = cacheType.create();
    }
    IntStream.range(0, keys.length).parallel().forEach(i -> keys[i] = generator.nextValue());
  }

  @Benchmark
  public Long getSpread(final LoadStaticZipfBenchmark.ThreadState threadState) {
    return benchmarkFunction.apply(keys[threadState.index++ & MASK]);
  }
}
