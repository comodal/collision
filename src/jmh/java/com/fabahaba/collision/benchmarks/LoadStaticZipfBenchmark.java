package com.fabahaba.collision.benchmarks;

import com.fabahaba.collision.cache.CollisionCache;
import com.fabahaba.collision.cache.LoadingCollisionBuilder;
import com.github.benmanes.caffeine.cache.LoadingCache;

import org.cache2k.Cache;
import org.cache2k.Cache2kBuilder;
import org.cache2k.integration.CacheLoader;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.IntStream;

@State(Scope.Benchmark)
@Threads(32)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 10)
@Measurement(iterations = 20)
public class LoadStaticZipfBenchmark {

  @Param({
             //"Cache2k",
             "Caffeine",
             "Collision",
             "Collision_Load_Atomic"
         })
  BenchmarkFunctionFactory cacheType;
  Function<Long, Long> benchmarkFunction;
  Long[] keys = new Long[SIZE];

  @State(Scope.Thread)
  public static class ThreadState {

    int index = ThreadLocalRandom.current().nextInt();
  }

  static final int SIZE = 1 << 20;
  static final int MASK = SIZE - 1;
  static final int CAPACITY = 1 << 17;
  static final int ITEMS = SIZE / 3;

  @Setup
  public void setup() {
    this.benchmarkFunction = cacheType.create();
    final ScrambledZipfGenerator generator = new ScrambledZipfGenerator(ITEMS);
    IntStream.range(0, keys.length).parallel().forEach(i -> {
      final Long key = generator.nextValue();
      keys[i] = key;
      if (!key.equals(benchmarkFunction.apply(key))) {
        throw new IllegalStateException(cacheType + " returned invalid value.");
      }
    });
  }

  @Benchmark
  public Long spread(final ThreadState threadState) {
    return benchmarkFunction.apply(keys[threadState.index++ & MASK]);
  }

  // have to sleep for at least 1ms, so amortize 10 microsecond disk calls,
  // by sleeping (10 / 1000.0)% of calls.
  private static final double SLEEP_RAND = 10 / 1000.0;

  private static void amortizedSleep() {
    try {
      if (Math.random() < SLEEP_RAND) {
        Thread.sleep(1);
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private static Long punishMiss(final Long num) {
    final double cubed = Math.pow(num, 3);
    return (long) Math.cbrt(cubed);
  }

  private static final Function<Long, Long> LOADER = num -> {
    amortizedSleep();
    return punishMiss(num);
  };

  public enum BenchmarkFunctionFactory {
    Cache2k {
      @Override
      public Function<Long, Long> create() {
        final Cache<Long, Long> cache = Cache2kBuilder
            .of(Long.class, Long.class)
            .disableStatistics(true)
            .entryCapacity(CAPACITY)
            .loader(new CacheLoader<>() {
              public Long load(final Long key) throws Exception {
                amortizedSleep();
                return punishMiss(key);
              }
            }).build();
        System.out.println(cache);
        return cache::get;
      }
    },
    Caffeine {
      @Override
      public Function<Long, Long> create() {
        final LoadingCache<Long, Long> cache = com.github.benmanes.caffeine.cache.Caffeine
            .newBuilder()
            .initialCapacity(CAPACITY)
            .maximumSize(CAPACITY)
            .build(LOADER::apply);
        return cache::get;
      }
    },
    Collision {
      @Override
      public Function<Long, Long> create() {
        final CollisionCache<Long, Long> cache = startCollision()
            .buildSparse(3.0);
        System.out.println(cache);
        return cache::getAggressive;
      }
    },
    Collision_Load_Atomic {
      @Override
      public Function<Long, Long> create() {
        final CollisionCache<Long, Long> cache = startCollision()
            .buildSparse(5.0);
        System.out.println(cache);
        return key -> cache.get(key, LOADER);
      }
    };

    public abstract Function<Long, Long> create();
  }

  private static LoadingCollisionBuilder<Long, Long, Long> startCollision() {
    return CollisionCache
        .withCapacity(CAPACITY, Long.class)
        .setStrictCapacity(true)
        .setLoader(
            key -> {
              amortizedSleep();
              return key;
            }, (key, num) -> punishMiss(num));
  }

  public static void main(final String[] args) {
    if (args.length == 0) {
      runForMemProfiler("Collision_Load_Atomic");
    }
    for (final String arg : args) {
      runForMemProfiler(arg);
    }
  }

  private static double memEstimate(final Runtime rt) {
    return (rt.totalMemory() - rt.freeMemory()) / 1048576.0;
  }

  private static void runForMemProfiler(final String cacheType) {
    final BenchmarkFunctionFactory cacheFactory = BenchmarkFunctionFactory.valueOf(cacheType);
    final Long[] keys = new Long[SIZE];
    final ScrambledZipfGenerator generator = new ScrambledZipfGenerator(ITEMS);
    for (int i = 0;i < keys.length;i++) {
      keys[i] = generator.nextValue();
    }
    final Runtime rt = Runtime.getRuntime();
    rt.gc();
    Thread.yield();
    System.out.println("Estimating memory usage for " + cacheType);
    final double baseUsage = memEstimate(rt);
    System.out.format("%.2fmB base usage.%n", baseUsage);
    final Function<Long, Long> benchmarkFunction = cacheFactory.create();
    for (final Long key : keys) {
      if (!key.equals(benchmarkFunction.apply(key))) {
        throw new IllegalStateException(cacheType + " returned invalid value.");
      }
    }

    for (;;) {
      System.out.format("%.2fmB%n", memEstimate(rt) - baseUsage);
      final Long key = keys[(int) (Math.random() * keys.length)];
      System.out.println(key + " -> " + benchmarkFunction.apply(key));
      try {
        rt.gc();
        Thread.sleep(2000);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
