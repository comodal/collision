package systems.comodal.collision.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static systems.comodal.collision.cache.AtomicLogCounters.MAX_COUNT;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.Before;
import org.junit.Test;

public class AtomicLogCountersTest {

  private static final int numCounters = 8;
  private static final int initCount = 3;
  private static final int maxCounterVal = Integer.highestOneBit(1_048_576 - 1) << 1;
  private AtomicLogCounters counters;

  @Before
  public void before() {
    this.counters = AtomicLogCounters.create(numCounters, initCount, maxCounterVal);
  }

  @Test
  public void testCounters() throws ExecutionException, InterruptedException {
    int expected = (int) ((((256 * 256) / 2.0) / maxCounterVal) * 100.0);
    if (expected % 2 == 1) {
      expected++;
    }

    final int counterIndex = 3;
    counters.initializeOpaque(counterIndex);

    double deltaPercentage = .2;
    double minDelta = 7;

    int numThreads = Math.max(16, Runtime.getRuntime().availableProcessors() * 2);
    final ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    final Runnable increment = () -> counters.increment(counterIndex);

    for (int i = 0, log = 1 << 8, toggle = 0, previousExpected = 0; ; ) {
      final Future[] futures = new Future[log - i];
      final CountDownLatch countDownLatch = new CountDownLatch(numThreads + 1);
      for (int j = i, index = 0; j < log; j++) {
        if (index < numThreads) {
          futures[index++] = executor.submit(() -> {
            try {
              countDownLatch.countDown();
              countDownLatch.await();
            } catch (final InterruptedException e) {
              Thread.interrupted();
              throw new RuntimeException(e);
            }
            increment.run();
          });
        } else {
          futures[index++] = executor.submit(increment);
        }
      }
      countDownLatch.countDown();
      for (int j = futures.length; j > 0; ) {
        futures[--j].get();
      }

      final int actual = counters.getOpaque(counterIndex);
      final double delta = minDelta + expected * deltaPercentage;
      System.out.printf("%d <> %d +- %.1f%n", expected, actual, delta);
      assertTrue(actual >= previousExpected);
      assertEquals(expected, actual, delta);
      if (previousExpected == MAX_COUNT) {
        break;
      }
      i = log;
      log <<= 1;
      final int nextExpected = expected + (toggle++ % 2 == 0 ? expected / 2 : previousExpected / 2);
      previousExpected = expected;
      expected = Math.min(MAX_COUNT, nextExpected);
      if (previousExpected == MAX_COUNT) {
        minDelta = 0;
        deltaPercentage = 0.0;
      } else {
        deltaPercentage -= .01;
      }
    }
    executor.shutdown();
    for (int i = 0; i < numCounters; ++i) {
      if (i == counterIndex) {
        assertEquals(MAX_COUNT, counters.getOpaque(i));
      } else {
        assertEquals(0, counters.getOpaque(i));
      }
    }
  }

  @Test
  public void testDecay() {
    int initCount = 2;
    for (int i = 0; i < numCounters; ++i) {
      counters.setOpaque(i, initCount);
      assertEquals(initCount, counters.getOpaque(i));
      initCount = Math.min(MAX_COUNT, initCount << 1);
    }

    for (int i = 0,
        counterIndex = 7,
        decayed = MAX_COUNT,
        iterations = Integer.numberOfTrailingZeros(256) + 1;
        i < iterations; ++i) {
      counters.decay(0, numCounters, -1);
      decayed /= 2;
      assertEquals(decayed, counters.getOpaque(counterIndex));
    }

    for (int i = 0; i < numCounters; ++i) {
      assertEquals(0, counters.getOpaque(i));
    }
  }
}
