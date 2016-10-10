package com.fabahaba.collision.cache;

import org.junit.Test;

import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

public class LogCounterTest {

  @Test
  public void testCounters() {
    final int numCounters = 8;
    final int initCount = 3;
    final int maxCounterVal = Integer.highestOneBit(1_048_576 - 1) << 1;
    int expected = (int) ((((256 * 256) / 2.0) / maxCounterVal) * 100.0);
    if (expected % 2 == 1) {
      expected++;
    }
    final LogCounters counters = new LogCounters(numCounters, initCount, maxCounterVal);
    final int max = 255;
    final int counterIndex = 3;
    counters.initCount(counterIndex);

    double deltaPercentage = .15;
    final double minDelta = 8;

    for (int i = 0, log = 1 << 8, toggle = 0, previousExpected = 0;;) {
      IntStream.range(i, log).parallel().forEach(j -> counters.atomicIncrement(counterIndex));
      final int actual = counters.getAcquireCount(counterIndex);
      final double delta = minDelta + expected * deltaPercentage;
      System.out.printf("%d <> %d +- %.1f%n", expected, actual, delta);
      assertEquals(expected, actual, delta);
      if (previousExpected == max) {
        break;
      }
      i = log;
      log <<= 1;
      final int nextExpected = expected + (toggle++ % 2 == 0 ? expected / 2 : previousExpected / 2);
      previousExpected = expected;
      expected = Math.min(max, nextExpected);
      deltaPercentage -= .01;
    }

    for (int i = 0;i < numCounters;++i) {
      if (i == counterIndex) {
        assertEquals(max, counters.getAcquireCount(i));
      } else {
        assertEquals(0, counters.getAcquireCount(i));
      }
    }
  }

  static final class LogCounters extends LogCounterCache {

    LogCounters(final int numCounters, final int initCount, final int maxCounterVal) {
      super(new byte[numCounters], initCount, LogCounterCache.calcLogFactorShift(maxCounterVal));
    }

    void initCount(final int index) {
      BA.setRelease(counters, index, initCount);
    }

    int getAcquireCount(final int index) {
      return ((int) BA.getAcquire(counters, index)) & 0xff;
    }
  }
}
