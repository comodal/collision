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

@State(Scope.Benchmark)
@Threads(Threads.MAX)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 10)
@Measurement(iterations = 20)
public class LoadingBenchmark {

  @Param({
             "Cache2k",
             "Caffeine",
             //"Collision_No_Keys",
             "Collision_With_Keys",
             "Collision_With_Keys_Atomic"
         })
  BenchmarkFunctionFactory cacheType;
  private Function<Long, Long> benchmarkFunction;
  private Long[] keys;

  @State(Scope.Thread)
  public static class ThreadState {

    int index = ThreadLocalRandom.current().nextInt();
  }

  private static final int SIZE = 1 << 20;
  private static final int MASK = SIZE - 1;
  private static final int CAPACITY = 1 << 17;
  private static final int ITEMS = SIZE / 3;

  @Setup
  public void setup() {
    this.keys = new Long[SIZE];
    final ScrambledZipfianGenerator generator = new ScrambledZipfianGenerator(ITEMS);
    for (int i = 0;i < SIZE;i++) {
      this.keys[i] = generator.nextValue();
    }
    this.benchmarkFunction = cacheType.create();
    for (final Long key : keys) {
      if (!key.equals(benchmarkFunction.apply(key))) {
        throw new IllegalStateException(cacheType + " returned invalid value.");
      }
    }
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

  private static final Function<Long, Long> LOADER = num -> {
    amortizedSleep();
    Math.pow(num, 3);
    return num;
  };

  public enum BenchmarkFunctionFactory {
    Cache2k {
      @Override
      Function<Long, Long> create() {
        final Cache<Long, Long> cache = Cache2kBuilder
            .of(Long.class, Long.class)
            .disableStatistics(true)
            .entryCapacity(CAPACITY)
            .loader(new CacheLoader<>() {
              public Long load(final Long key) throws Exception {
                amortizedSleep();
                Math.pow(key, 3);
                return key;
              }
            }).build();
        return cache::get;
      }
    },
    Caffeine {
      @Override
      Function<Long, Long> create() {
        final LoadingCache<Long, Long> cache = com.github.benmanes.caffeine.cache.Caffeine
            .newBuilder()
            .initialCapacity(CAPACITY)
            .maximumSize(CAPACITY)
            .build(LOADER::apply);
        return cache::get;
      }
    },
    Collision_No_Keys {
      @Override
      Function<Long, Long> create() {
        final CollisionCache<Long, Long> cache = startCollision()
            .setStoreKeys(false)
            .buildSparse(3.0);
        return cache::get;
      }
    },
    Collision_With_Keys {
      @Override
      Function<Long, Long> create() {
        final CollisionCache<Long, Long> cache = startCollision()
            .buildSparse(3.0);
        return cache::get;
      }
    },
    Collision_With_Keys_Atomic {
      @Override
      Function<Long, Long> create() {
        final CollisionCache<Long, Long> cache = startCollision()
            .buildSparse(3.0);
        return cache::getLoadAtomic;
      }
    };

    abstract Function<Long, Long> create();
  }

  private static LoadingCollisionBuilder<Long, Long, Long> startCollision() {
    return CollisionCache
        .withCapacity(CAPACITY, Long.class)
        .setStrictCapacity(true)
        .setLoader(
            key -> {
              amortizedSleep();
              return key;
            }, (key, num) -> {
              Math.pow(num, 3);
              return num;
            });
  }
}
