package systems.comodal.collision.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.stream.IntStream;
import org.junit.Before;
import org.junit.Test;

public class AtomicLogCountersTest {

  private static final int numCounters = 8;
  private static final int initCount = 3;
  private static final int maxCounterVal = Integer.highestOneBit(1_048_576 - 1) << 1;
  private AtomicLogCounters counters;

  @Before
  public void before() {
    this.counters = new AtomicLogCounters(numCounters, initCount, maxCounterVal);
  }

  @Test
  public void testCounters() {
    int expected = (int) ((((256 * 256) / 2.0) / maxCounterVal) * 100.0);
    if (expected % 2 == 1) {
      expected++;
    }

    final int counterIndex = 3;
    counters.initCount(counterIndex);

    double deltaPercentage = .2;
    double minDelta = 7;

    for (int i = 0, log = 1 << 8, toggle = 0, previousExpected = 0; ; ) {
      IntStream.range(i, log).parallel().forEach(j -> counters.atomicIncrement(counterIndex));
      final int actual = counters.getOpaqueCount(counterIndex);
      final double delta = minDelta + expected * deltaPercentage;
      System.out.printf("%d <> %d +- %.1f%n", expected, actual, delta);
      assertTrue(actual >= previousExpected);
      assertEquals(expected, actual, delta);
      if (previousExpected == AtomicLogCounters.MAX_COUNT) {
        break;
      }
      i = log;
      log <<= 1;
      final int nextExpected = expected + (toggle++ % 2 == 0 ? expected / 2 : previousExpected / 2);
      previousExpected = expected;
      expected = Math.min(AtomicLogCounters.MAX_COUNT, nextExpected);
      if (previousExpected == AtomicLogCounters.MAX_COUNT) {
        minDelta = 0;
        deltaPercentage = 0.0;
      } else {
        deltaPercentage -= .01;
      }
    }

    for (int i = 0; i < numCounters; ++i) {
      if (i == counterIndex) {
        assertEquals(AtomicLogCounters.MAX_COUNT, counters.getOpaqueCount(i));
      } else {
        assertEquals(0, counters.getOpaqueCount(i));
      }
    }
  }

  @Test
  public void testDecay() {
    int initCount = 2;
    for (int i = 0; i < numCounters; ++i) {
      counters.initCount(i, initCount);
      assertEquals(initCount, counters.getOpaqueCount(i));
      initCount = Math.min(AtomicLogCounters.MAX_COUNT, initCount << 1);
    }

    for (int i = 0,
        counterIndex = 7,
        decayed = AtomicLogCounters.MAX_COUNT,
        iterations = Integer.numberOfTrailingZeros(256) + 1;
        i < iterations; ++i) {
      counters.decay(0, numCounters, -1);
      decayed /= 2;
      assertEquals(decayed, counters.getOpaqueCount(counterIndex));
    }

    for (int i = 0; i < numCounters; ++i) {
      assertEquals(0, counters.getOpaqueCount(i));
    }
  }
}
