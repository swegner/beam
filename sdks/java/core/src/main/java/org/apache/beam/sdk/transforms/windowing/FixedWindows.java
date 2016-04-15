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

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.display.DisplayData;

import com.google.auto.value.AutoValue;

import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * A {@link WindowFn} that windows values into fixed-size timestamp-based windows.
 *
 * <p>For example, in order to partition the data into 10 minute windows:
 * <pre> {@code
 * PCollection<Integer> items = ...;
 * PCollection<Integer> windowedItems = items.apply(
 *   Window.<Integer>into(FixedWindows.of(Duration.standardMinutes(10))));
 * } </pre>
 */
@AutoValue
public abstract class FixedWindows extends PartitioningWindowFn<Object, IntervalWindow> {

  /**
   * Size of this window.
   */
  public abstract Duration getSize();

  /**
   * Offset of this window.  Windows start at time
   * N * size + offset, where 0 is the epoch.
   */
  public abstract Duration getOffset();

  /**
   * Partitions the timestamp space into half-open intervals of the form
   * [N * size, (N + 1) * size), where 0 is the epoch.
   */
  public static FixedWindows of(Duration size) {
    return FixedWindows.of(size, Duration.ZERO);
  }

  /**
   * Partitions the timestamp space into half-open intervals of the form
   * [N * size + offset, (N + 1) * size + offset),
   * where 0 is the epoch.
   *
   * @throws IllegalArgumentException if offset is not in [0, size)
   */
  public FixedWindows withOffset(Duration offset) {
    return FixedWindows.of(getSize(), offset);
  }

  private static FixedWindows of(Duration size, Duration offset) {
    if (offset.isShorterThan(Duration.ZERO) || !offset.isShorterThan(size)) {
      throw new IllegalArgumentException(
          "FixedWindows WindowingStrategies must have 0 <= offset < size");
    }

    return new AutoValue_FixedWindows(size, offset);
  }

  @Override
  public IntervalWindow assignWindow(Instant timestamp) {
    long start = timestamp.getMillis()
        - timestamp.plus(getSize()).minus(getOffset()).getMillis() % getSize().getMillis();
    return new IntervalWindow(new Instant(start), getSize());
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    builder
        .add("size", getSize())
        .addIfNotDefault("offset", getOffset(), Duration.ZERO);
  }

  @Override
  public Coder<IntervalWindow> windowCoder() {
    return IntervalWindow.getCoder();
  }

  @Override
  public boolean isCompatible(WindowFn<?, ?> other) {
    return this.equals(other);
  }
}
