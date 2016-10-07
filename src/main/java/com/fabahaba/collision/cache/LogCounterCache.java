package com.fabahaba.collision.cache;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.ThreadLocalRandom;

abstract class LogCounterCache {

  static final VarHandle BA = MethodHandles.arrayElementVarHandle(byte[].class);
  static final VarHandle OA = MethodHandles.arrayElementVarHandle(Object[].class);

  final byte[] counters;
  final byte initCount;
  private final int pow2LogFactor;

  LogCounterCache(final byte[] counters, final int initCount, final int pow2LogFactor) {
    this.counters = counters;
    this.initCount = (byte) initCount;
    this.pow2LogFactor = pow2LogFactor;
  }

  /**
   * Probabilistically increments a relatively large counter, represented from
   * {@code initCount} to 255.  The probability of an increment decreases at a rate of
   * {@code (1 / (counters[index] * maxRelativeCount / (256^2 / 2)))}.
   *
   * @param index Array index of the counter to increment.
   */
  final void atomicIncrement(final int index) {
    for (int count = ((int) BA.getAcquire(counters, index)) & 0xff;count != 0xff;) {
      if (count <= initCount) {
        if (BA.compareAndSet(counters, index, (byte) count, (byte) (count + 1))) {
          return;
        }
        count = ((int) BA.getAcquire(counters, index)) & 0xff;
        continue;
      }
      final double rand = 1.0 / ThreadLocalRandom.current().nextDouble();
      if (rand < count - initCount << pow2LogFactor) {
        return;
      }
      while (!BA.compareAndSet(counters, index, (byte) count, (byte) (count + 1))) {
        count = ((int) BA.getAcquire(counters, index)) & 0xff;
        if (count == 0xff || (count > initCount && rand < count - initCount << pow2LogFactor)) {
          return;
        }
      }
      return;
    }
  }

  /**
   * Used in conjunction with {@link #atomicIncrement atomicIncrement} as a multiplication
   * factor to decrease the probability of a counter increment as the counter increases.
   *
   * @param maxCount The relative max count.  Once a counter is incremented this many times its
   *                 val should be 255.
   * @return The power of two multiplication factor as the number of bits to shift.
   */
  static int calcLogFactorShift(final int maxCount) {
    // Divide next highest power of 2 by 32,768... (256^2 / 2).
    // Then get the number of bits to shift for efficiency.
    // The result of this factor will cause the count to be 255 after maxCount increments.
    return Integer.numberOfTrailingZeros(Integer.highestOneBit(maxCount - 1) >> 14);
  }

  final void decay(final int counterOffset, final int maxCounterIndex, final int skipIndex) {
    int counterIndex = counterOffset;
    do {
      if (counterIndex == skipIndex) {
        continue;
      }
      int count = ((int) BA.getAcquire(counters, counterIndex)) & 0xff;
      if (count == 0) {
        continue;
      }
      // Counter misses may occur between these two calls.
      BA.setRelease(counters, counterIndex, (byte) (count >> 1));
    } while (++counterIndex < maxCounterIndex);
  }
}
