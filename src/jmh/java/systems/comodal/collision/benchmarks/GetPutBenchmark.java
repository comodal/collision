package systems.comodal.collision.benchmarks;

import com.github.benmanes.caffeine.cache.Cache;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
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
import systems.comodal.collision.cache.CollisionCache;

@State(Scope.Group)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 10)
@Measurement(iterations = 20)
public class GetPutBenchmark {

  private static final int SIZE = (2 << 14);
  private static final int MASK = SIZE - 1;
  private static final int ITEMS = SIZE / 3;
  @Param({
      "Cache2k",
      "Caffeine",
      "Collision"
  })
  private CacheFactory cacheType;
  private GetPutCache<Long, Boolean> cache;
  private Long[] keys;

  @Setup
  public void setup() {
    keys = new Long[SIZE];
    final int capacity = SIZE / 2;
    cache = cacheType.create(capacity);
    final ScrambledZipfGenerator generator = new ScrambledZipfGenerator(ITEMS);
    IntStream.range(0, keys.length).parallel().forEach(i -> {
      final Long key = generator.nextValue();
      keys[i] = key;
      cache.put(key, Boolean.TRUE);
    });
  }

  @Benchmark
  @Group("readOnly")
  @GroupThreads(16)
  public Boolean readOnlyGet(LoadStaticZipfBenchmark.ThreadState threadState) {
    return cache.get(keys[threadState.index++ & MASK]);
  }

  @Benchmark
  @Group("writeOnly")
  @GroupThreads(16)
  public Boolean writeOnlyPut(LoadStaticZipfBenchmark.ThreadState threadState) {
    return cache.put(keys[threadState.index++ & MASK], Boolean.TRUE);
  }

  @Benchmark
  @Group("readWrite")
  @GroupThreads(12)
  public Boolean readWriteGet(LoadStaticZipfBenchmark.ThreadState threadState) {
    return cache.get(keys[threadState.index++ & MASK]);
  }

  @Benchmark
  @Group("readWrite")
  @GroupThreads(4)
  public Boolean readWritePut(LoadStaticZipfBenchmark.ThreadState threadState) {
    return cache.put(keys[threadState.index++ & MASK], Boolean.TRUE);
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
          public V put(final K key, final V val) {
            cache.put(key, val);
            return val;
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
          public V put(final K key, final V val) {
            cache.put(key, val);
            return val;
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
          public V put(final K key, final V val) {
            return cache.putReplace(key, val);
          }
        };
      }
    };

    abstract <K, V> GetPutCache<K, V> create(final int capacity);
  }

  private interface GetPutCache<K, V> {

    V get(final K key);

    V put(final K key, final V val);
  }
}
