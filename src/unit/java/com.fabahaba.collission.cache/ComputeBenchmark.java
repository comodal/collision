package com.fabahaba.collission.cache;

import com.fabahaba.collision.cache.CollisionCache;
import com.fabahaba.collision.cache.LoadingCollisionBuilder;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

import org.cache2k.Cache;
import org.cache2k.Cache2kBuilder;
import org.cache2k.integration.CacheLoader;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

final class ComputeBenchmark {

  // have to sleep for at least 1ms, so amortize 10 microsecond disk calls,
  // by sleeping (10 / 1000.0)% of calls.
  static final double SLEEP_RAND = 10 / 1000.0;

  static final int NUM_ITERS = 10;
  static final int SIZE = 1 << 20;
  static final int CAP = (int) ((1 << 17) * 1.0);
  static final int ITEMS = SIZE / 3;
  private static final Integer[] VALUES = new Integer[SIZE];

  static {
    final ScrambledZipfianGenerator generator = new ScrambledZipfianGenerator(ITEMS);
    for (int i = 0;i < SIZE;i++) {
      VALUES[i] = (int) generator.nextValue();
    }
  }

  static final Function<Integer, Integer> LOADER = num -> {
    try {
      if (Math.random() < SLEEP_RAND) {
        Thread.sleep(1);
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    Math.pow(num, 3);
    return num;
  };

  private final Object cache;
  private final Function<Integer, Integer> benchmarkFunction;

  ComputeBenchmark(final Object cache, final Function<Integer, Integer> benchmarkFunction) {
    this.cache = cache;
    this.benchmarkFunction = benchmarkFunction;
  }

  static double initMemUsage() {
    final Runtime runtime = Runtime.getRuntime();
    runtime.gc();
    Thread.yield();
    final double startMemUsage = (runtime.totalMemory() - runtime.freeMemory()) / (1048576.0);
    System.out.format("%.2fmB%n", startMemUsage);
    return startMemUsage;
  }

  public void test(final double startMemUsage) {
    runTest(startMemUsage, () -> {
      for (final Integer val : VALUES) {
        if (!benchmarkFunction.apply(val).equals(val)) {
          throw new IllegalStateException(String.valueOf(val));
        }
      }
    });
    System.out.println(cache);
  }

  public void testParallel(final double startMemUsage) {
    runTest(startMemUsage, () -> Arrays.stream(VALUES).parallel().forEach(val -> {
      if (!benchmarkFunction.apply(val).equals(val)) {
        throw new IllegalStateException(String.valueOf(val));
      }
    }));
    System.out.println(cache);
  }

  private static void runTest(final double startMemUsage, final Runnable test) {
    System.out.format("Testing %d items %d times with a capacity of %d.%n", ITEMS,
        SIZE, CAP);
    final long start = System.nanoTime();
    for (int i = 0;i < NUM_ITERS;++i) {
      test.run();
    }
    final long time = System.nanoTime() - start;
    System.out.println(TimeUnit.MILLISECONDS.convert(time, TimeUnit.NANOSECONDS) + "ms");
    final Runtime runtime = Runtime.getRuntime();
    runtime.gc();
    Thread.yield();
    final double endMemUsage = (runtime.totalMemory() - runtime.freeMemory()) / (1048576.0);
    System.out.format("%.2fmB%n", endMemUsage);
    System.out.format("%.2fmB%n", endMemUsage - startMemUsage);
  }

  static ComputeBenchmark createCache2k() {
    final Cache<Integer, Integer> cache = Cache2kBuilder.of(Integer.class, Integer.class)
        .disableStatistics(true)
        .entryCapacity(CAP)
        .sharpExpiry(false)
        .strictEviction(false).loader(new CacheLoader<Integer, Integer>() {
          public Integer load(final Integer key) throws Exception {
            return LOADER.apply(key);
          }
        }).build();
    return new ComputeBenchmark(cache, key -> cache.get(key));
  }

  static ComputeBenchmark createCaffeine() {
    final LoadingCache<Integer, Integer> cache = Caffeine.newBuilder()
        .initialCapacity(CAP)
        .maximumSize(CAP).build(LOADER::apply);
    return new ComputeBenchmark(cache, key -> cache.get(key));
  }

  static LoadingCollisionBuilder<Integer, Integer, Integer> startCollision() {
    return CollisionCache.withCapacity(CAP, Integer.class)
        .setLoader(
            num -> {
              try {
                if (Math.random() < SLEEP_RAND) {
                  Thread.sleep(1);
                }
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
              return num;
            },
            (key, num) -> {
              Math.pow(num, 3);
              return num;
            });
  }

  static ComputeBenchmark createPackedCollision() {
    final CollisionCache<Integer, Integer> cache = startCollision()
        .setStoreKeys(false).buildPacked();
    return new ComputeBenchmark(cache, key -> cache.get(key));
  }

  static ComputeBenchmark createPackedEntryCollision() {
    final CollisionCache<Integer, Integer> cache = startCollision().buildPacked();
    return new ComputeBenchmark(cache, key -> cache.get(key));
  }

  static ComputeBenchmark createSparseCollision() {
    final CollisionCache<Integer, Integer> cache = startCollision()
        .setStoreKeys(false).setStrictCapacity(false).buildSparse(3.0);
    return new ComputeBenchmark(cache, key -> cache.get(key));
  }

  static ComputeBenchmark createSparseEntryCollision() {
    final CollisionCache<Integer, Integer> cache = startCollision().buildSparse(3.0);
    return new ComputeBenchmark(cache, key -> cache.get(key));
  }

  static ComputeBenchmark createConcurrentMap() {
    final Map<Integer, Integer> cache = new ConcurrentHashMap<>(CAP);
    return new ComputeBenchmark(cache, key -> cache.computeIfAbsent(key, LOADER::apply));
  }

  public static void main(final String[] args) {
    final double memUsage = initMemUsage();
    //createCache2k().testParallel(memUsage);
    //createCaffeine().testParallel(memUsage);
    //createPackedCollision().testParallel(memUsage);
    //createPackedEntryCollision().testParallel(memUsage);
    createSparseCollision().testParallel(memUsage);
    //createSparseEntryCollision().testParallel(memUsage);
    //createConcurrentMap().testParallel(memUsage);
  }
}
