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

import java.util.Arrays;
import java.util.Collection;

/**
 * A {@link WindowFn} windowing values into sessions separated by {@link #getGapDuration}-long
 * periods with no elements.
 *
 * <p>For example, in order to window data into session with at least 10 minute
 * gaps in between them:
 * <pre> {@code
 * PCollection<Integer> pc = ...;
 * PCollection<Integer> windowed_pc = pc.apply(
 *   Window.<Integer>into(Sessions.withGapDuration(Duration.standardMinutes(10))));
 * } </pre>
 */
@AutoValue
public abstract class Sessions extends WindowFn<Object, IntervalWindow> {
  /**
   * Duration of the gaps between sessions.
   */
  public abstract Duration getGapDuration();

  /**
   * Creates a {@code Sessions} {@link WindowFn} with the specified gap duration.
   */
  public static Sessions withGapDuration(Duration gapDuration) {
    return new AutoValue_Sessions(gapDuration);
  }

  @Override
  public Collection<IntervalWindow> assignWindows(AssignContext c) {
    // Assign each element into a window from its timestamp until gapDuration in the
    // future.  Overlapping windows (representing elements within gapDuration of
    // each other) will be merged.
    return Arrays.asList(new IntervalWindow(c.timestamp(), getGapDuration()));
  }

  @Override
  public void mergeWindows(MergeContext c) throws Exception {
    MergeOverlappingIntervalWindows.mergeWindows(c);
  }

  @Override
  public Coder<IntervalWindow> windowCoder() {
    return IntervalWindow.getCoder();
  }

  @Override
  public boolean isCompatible(WindowFn<?, ?> other) {
    return other instanceof Sessions;
  }

  @Override
  public IntervalWindow getSideInputWindow(BoundedWindow window) {
    throw new UnsupportedOperationException("Sessions is not allowed in side inputs");
  }

  @Experimental(Kind.OUTPUT_TIME)
  @Override
  public OutputTimeFn<? super IntervalWindow> getOutputTimeFn() {
    return OutputTimeFns.outputAtEarliestInputTimestamp();
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    builder.add("gapDuration", getGapDuration());
  }
}
