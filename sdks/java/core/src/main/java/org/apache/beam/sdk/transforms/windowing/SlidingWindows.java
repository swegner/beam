/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.transforms.windowing;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.display.DisplayData;

import com.google.auto.value.AutoValue;

import org.joda.time.Duration;
import org.joda.time.Instant;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A {@link WindowFn} that windows values into possibly overlapping fixed-size
 * timestamp-based windows.
 *
 * <p>For example, in order to window data into 10 minute windows that
 * update every minute:
 * <pre> {@code
 * PCollection<Integer> items = ...;
 * PCollection<Integer> windowedItems = items.apply(
 *   Window.<Integer>into(SlidingWindows.of(Duration.standardMinutes(10))));
 * } </pre>
 */
@AutoValue
public abstract class SlidingWindows extends NonMergingWindowFn<Object, IntervalWindow> {

  /**
   * Amount of time between generated windows.
   */
  public abstract Duration getPeriod();

  /**
   * Size of the generated windows.
   */
  public abstract Duration getSize();

  /**
   * Offset of the generated windows.
   * Windows start at time N * start + offset, where 0 is the epoch.
   */
  public abstract Duration getOffset();


  /**
   * Assigns timestamps into half-open intervals of the form
   * [N * period, N * period + size), where 0 is the epoch.
   *
   * <p>If {@link SlidingWindows#every} is not called, the period defaults
   * to the largest time unit smaller than the given duration.  For example,
   * specifying a size of 5 seconds will result in a default period of 1 second.
   */
  public static SlidingWindows of(Duration size) {
    return SlidingWindows.of(getDefaultPeriod(size), size, Duration.ZERO);
  }

  private static SlidingWindows of(Duration period, Duration size, Duration offset) {
    if (offset.isShorterThan(Duration.ZERO)
        || !offset.isShorterThan(period)
        || !size.isLongerThan(Duration.ZERO)) {
      throw new IllegalArgumentException(
          "SlidingWindows WindowingStrategies must have 0 <= offset < period and 0 < size");
    }

    return new AutoValue_SlidingWindows(period, size, offset);
  }

  /**
   * Returns a new {@code SlidingWindows} with the original size, that assigns
   * timestamps into half-open intervals of the form
   * [N * period, N * period + size), where 0 is the epoch.
   */
  public SlidingWindows every(Duration period) {
    return SlidingWindows.of(period, getSize(), getOffset());
  }

  /**
   * Assigns timestamps into half-open intervals of the form
   * [N * period + offset, N * period + offset + size).
   *
   * @throws IllegalArgumentException if offset is not in [0, period)
   */
  public SlidingWindows withOffset(Duration offset) {
    return SlidingWindows.of(getPeriod(), getSize(), offset);
  }

  @Override
  public Coder<IntervalWindow> windowCoder() {
    return IntervalWindow.getCoder();
  }

  @Override
  public Collection<IntervalWindow> assignWindows(AssignContext c) {
    List<IntervalWindow> windows =
        new ArrayList<>((int) (getSize().getMillis() / getPeriod().getMillis()));
    Instant timestamp = c.timestamp();
    long lastStart = lastStartFor(timestamp);
    for (long start = lastStart;
         start > timestamp.minus(getSize()).getMillis();
         start -= getPeriod().getMillis()) {
      windows.add(new IntervalWindow(new Instant(start), getSize()));
    }
    return windows;
  }

  /**
   * Return the earliest window that contains the end of the main-input window.
   */
  @Override
  public IntervalWindow getSideInputWindow(final BoundedWindow window) {
    if (window instanceof GlobalWindow) {
      throw new IllegalArgumentException(
          "Attempted to get side input window for GlobalWindow from non-global WindowFn");
    }
    long lastStart = lastStartFor(window.maxTimestamp().minus(getSize()));
    return new IntervalWindow(new Instant(lastStart + getPeriod().getMillis()), getSize());
  }

  @Override
  public boolean isCompatible(WindowFn<?, ?> other) {
    return equals(other);
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    builder
        .add("size", getSize())
        .add("period", getPeriod())
        .add("offset", getOffset());
  }

  /**
   * Return the last start of a sliding window that contains the timestamp.
   */
  private long lastStartFor(Instant timestamp) {
    return timestamp.getMillis()
        - timestamp.plus(getPeriod()).minus(getOffset()).getMillis() % getPeriod().getMillis();
  }

  static Duration getDefaultPeriod(Duration size) {
    if (size.isLongerThan(Duration.standardHours(1))) {
      return Duration.standardHours(1);
    }
    if (size.isLongerThan(Duration.standardMinutes(1))) {
      return Duration.standardMinutes(1);
    }
    if (size.isLongerThan(Duration.standardSeconds(1))) {
      return Duration.standardSeconds(1);
    }
    return Duration.millis(1);
  }

  /**
   * Ensures that later sliding windows have an output time that is past the end of earlier windows.
   *
   * <p>If this is the earliest sliding window containing {@code inputTimestamp}, that's fine.
   * Otherwise, we pick the earliest time that doesn't overlap with earlier windows.
   */
  @Experimental(Kind.OUTPUT_TIME)
  @Override
  public OutputTimeFn<? super IntervalWindow> getOutputTimeFn() {
    return new OutputTimeFn.Defaults<BoundedWindow>() {
      @Override
      public Instant assignOutputTime(Instant inputTimestamp, BoundedWindow window) {
        Instant startOfLastSegment = window.maxTimestamp().minus(getPeriod());
        return startOfLastSegment.isBefore(inputTimestamp)
            ? inputTimestamp
                : startOfLastSegment.plus(1);
      }

      @Override
      public boolean dependsOnlyOnEarliestInputTimestamp() {
        return true;
      }
    };
  }
}
