package com.fabahaba.collision.benchmarks;

import com.fabahaba.collision.cache.CollisionCache;
import com.github.benmanes.caffeine.cache.Cache;

import org.cache2k.Cache2kBuilder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@State(Scope.Group)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 10)
@Measurement(iterations = 20)
public class GetPutBenchmark {

  @Param({
             "Cache2k",
             "Caffeine",
             "Collision"
         })
  CacheFactory cacheFactory;
  private GetPutCache<Long, Boolean> cache;
  private Long[] keys;

  private static final int SIZE = (2 << 14);
  private static final int MASK = SIZE - 1;
  private static final int ITEMS = SIZE / 3;

  @State(Scope.Thread)
  public static class ThreadState {

    int index = ThreadLocalRandom.current().nextInt();
  }

  @Setup
  public void setup() {
    keys = new Long[SIZE];
    final int capacity = SIZE / 2;
    cache = cacheFactory.create(capacity);
    for (long i = 0;i < capacity;i++) {
      cache.put(i, Boolean.TRUE);
    }
    cache.clear();
    final ScrambledZipfianGenerator generator = new ScrambledZipfianGenerator(ITEMS);
    for (int i = 0;i < SIZE;i++) {
      final Long key = generator.nextValue();
      this.keys[i] = key;
      cache.put(key, Boolean.TRUE);
    }
  }

  @Benchmark
  @Group("readOnly")
  @GroupThreads(16)
  public Boolean readOnlyGet(ThreadState threadState) {
    return cache.get(keys[threadState.index++ & MASK]);
  }

  @Benchmark
  @Group("writeOnly")
  @GroupThreads(16)
  public void writeOnlyPut(ThreadState threadState) {
    cache.put(keys[threadState.index++ & MASK], Boolean.TRUE);
  }

  @Benchmark
  @Group("readWrite")
  @GroupThreads(12)
  public Boolean readWriteGet(ThreadState threadState) {
    return cache.get(keys[threadState.index++ & MASK]);
  }

  @Benchmark
  @Group("readWrite")
  @GroupThreads(4)
  public void readWritePut(ThreadState threadState) {
    cache.put(keys[threadState.index++ & MASK], Boolean.TRUE);
  }

  private interface GetPutCache<K, V> {

    V get(final K key);

    void put(final K key, final V val);

    void clear();
  }

  public enum CacheFactory {
    Cache2k {
      @Override
      @SuppressWarnings("unchecked")
      <K, V> GetPutCache<K, V> create(final int capacity) {
        final org.cache2k.Cache<K, V> cache = Cache2kBuilder
            .forUnknownTypes()
            .entryCapacity(capacity)
            .disableStatistics(true)
            .eternal(true)
            .build();
        return new GetPutCache<>() {

          @Override
          public V get(final K key) {
            return cache.peek(key);
          }

          @Override
          public void put(final K key, final V val) {
            cache.put(key, val);
          }

          @Override
          public void clear() {
            cache.clear();
          }
        };
      }
    },
    Caffeine {
      @Override
      <K, V> GetPutCache<K, V> create(final int capacity) {
        final Cache<K, V> cache = com.github.benmanes.caffeine.cache.Caffeine
            .newBuilder()
            .initialCapacity(capacity)
            .maximumSize(capacity)
            .build();
        return new GetPutCache<>() {

          @Override
          public V get(final K key) {
            return cache.getIfPresent(key);
          }

          @Override
          public void put(final K key, final V val) {
            cache.put(key, val);
          }

          @Override
          public void clear() {
            cache.invalidateAll();
          }
        };
      }
    },
    Collision {
      @Override
      <K, V> GetPutCache<K, V> create(final int capacity) {
        final CollisionCache<K, V> cache = CollisionCache
            .<V>withCapacity(capacity)
            .setStrictCapacity(true)
            .buildSparse(3.0);
        return new GetPutCache<>() {

          @Override
          public V get(final K key) {
            return cache.getIfPresent(key);
          }

          @Override
          public void put(final K key, final V val) {
            cache.putReplace(key, val);
          }

          @Override
          public void clear() {
            cache.clear();
          }
        };
      }
    };

    abstract <K, V> GetPutCache<K, V> create(final int capacity);
  }
}
