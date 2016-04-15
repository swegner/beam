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
package org.apache.beam.sdk.util;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.OutputTimeFn;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Window.ClosingBehavior;
import org.apache.beam.sdk.transforms.windowing.WindowFn;

import com.google.auto.value.AutoValue;
import com.google.common.base.MoreObjects;

import org.joda.time.Duration;

import java.io.Serializable;

/**
 * A {@code WindowingStrategy} describes the windowing behavior for a specific collection of values.
 * It has both a {@link WindowFn} describing how elements are assigned to windows and a
 * {@link Trigger} that controls when output is produced for each window.
 *
 * @param <T> type of elements being windowed
 * @param <W> {@link BoundedWindow} subclass used to represent the
 *            windows used by this {@code WindowingStrategy}
 */
@AutoValue
public abstract class WindowingStrategy<T, W extends BoundedWindow> implements Serializable {

  /**
   * The accumulation modes that can be used with windowing.
   */
  public enum AccumulationMode {
    DISCARDING_FIRED_PANES,
    ACCUMULATING_FIRED_PANES;
  }

  private static final Duration DEFAULT_ALLOWED_LATENESS = Duration.ZERO;
  private static final WindowingStrategy<Object, GlobalWindow> DEFAULT = of(new GlobalWindows());

  private static <T, W extends BoundedWindow> WindowingStrategy<T, W> of(
      WindowFn<T, W> windowFn,
      ExecutableTrigger trigger, boolean triggerSpecified,
      AccumulationMode mode, boolean modeSpecified,
      Duration allowedLateness, boolean allowedLatenessSpecified,
      OutputTimeFn<? super W> outputTimeFn, boolean outputTimeFnSpecified,
      ClosingBehavior closingBehavior) {
    return new AutoValue_WindowingStrategy<>(
        windowFn,
        trigger,
        triggerSpecified,
        allowedLateness,
        allowedLatenessSpecified,
        mode,
        modeSpecified,
        outputTimeFn,
        outputTimeFnSpecified,
        closingBehavior);
  }

  /**
   * Return a fully specified, default windowing strategy.
   */
  public static WindowingStrategy<Object, GlobalWindow> globalDefault() {
    return DEFAULT;
  }

  public static <T, W extends BoundedWindow> WindowingStrategy<T, W> of(WindowFn<T, W> windowFn) {
    return WindowingStrategy.of(windowFn,
        ExecutableTrigger.create(DefaultTrigger.<W>of()), false,
        AccumulationMode.DISCARDING_FIRED_PANES, false,
        DEFAULT_ALLOWED_LATENESS, false,
        windowFn.getOutputTimeFn(), false,
        ClosingBehavior.FIRE_IF_NON_EMPTY);
  }

  public abstract WindowFn<T, W> getWindowFn();
  public abstract ExecutableTrigger getTrigger();
  public abstract boolean isTriggerSpecified();
  public abstract Duration getAllowedLateness();
  public abstract boolean isAllowedLatenessSpecified();
  public abstract AccumulationMode getMode();
  public abstract boolean isModeSpecified();
  public abstract OutputTimeFn<? super W> getOutputTimeFn();
  public abstract boolean isOutputTimeFnSpecified();
  public abstract ClosingBehavior getClosingBehavior();

  /**
   * Returns a {@link WindowingStrategy} identical to {@code this} but with the trigger set to
   * {@code wildcardTrigger}.
   */
  public WindowingStrategy<T, W> withTrigger(Trigger trigger) {
    return WindowingStrategy.of(
        getWindowFn(),
        ExecutableTrigger.create(trigger), true,
        getMode(), isModeSpecified(),
        getAllowedLateness(), isAllowedLatenessSpecified(),
        getOutputTimeFn(), isOutputTimeFnSpecified(),
        getClosingBehavior());
  }

  /**
   * Returns a {@link WindowingStrategy} identical to {@code this} but with the accumulation mode
   * set to {@code mode}.
   */
  public WindowingStrategy<T, W> withMode(AccumulationMode mode) {
    return WindowingStrategy.of(
        getWindowFn(),
        getTrigger(), isTriggerSpecified(),
        mode, true,
        getAllowedLateness(), isAllowedLatenessSpecified(),
        getOutputTimeFn(), isOutputTimeFnSpecified(),
        getClosingBehavior());
  }

  /**
   * Returns a {@link WindowingStrategy} identical to {@code this} but with the window function
   * set to {@code wildcardWindowFn}.
   */
  public WindowingStrategy<T, W> withWindowFn(WindowFn<?, ?> wildcardWindowFn) {
    @SuppressWarnings("unchecked")
    WindowFn<T, W> typedWindowFn = (WindowFn<T, W>) wildcardWindowFn;

    // The onus of type correctness falls on the callee.
    @SuppressWarnings("unchecked")
    OutputTimeFn<? super W> newOutputTimeFn = (OutputTimeFn<? super W>)
        (isOutputTimeFnSpecified() ? getOutputTimeFn() : typedWindowFn.getOutputTimeFn());

    return WindowingStrategy.of(
        typedWindowFn,
        getTrigger(), isTriggerSpecified(),
        getMode(), isModeSpecified(),
        getAllowedLateness(), isAllowedLatenessSpecified(),
        newOutputTimeFn, isOutputTimeFnSpecified(),
        getClosingBehavior());
  }

  /**
   * Returns a {@link WindowingStrategy} identical to {@code this} but with the allowed lateness
   * set to {@code allowedLateness}.
   */
  public WindowingStrategy<T, W> withAllowedLateness(Duration allowedLateness) {
    return WindowingStrategy.of(
        getWindowFn(),
        getTrigger(), isTriggerSpecified(),
        getMode(), isModeSpecified(),
        allowedLateness, true,
        getOutputTimeFn(), isOutputTimeFnSpecified(),
        getClosingBehavior());
  }

  public WindowingStrategy<T, W> withClosingBehavior(ClosingBehavior closingBehavior) {
    return WindowingStrategy.of(
        getWindowFn(),
        getTrigger(), isTriggerSpecified(),
        getMode(), isModeSpecified(),
        getAllowedLateness(), isAllowedLatenessSpecified(),
        getOutputTimeFn(), isOutputTimeFnSpecified(),
        closingBehavior);
  }

  @Experimental(Experimental.Kind.OUTPUT_TIME)
  public WindowingStrategy<T, W> withOutputTimeFn(OutputTimeFn<?> outputTimeFn) {

    @SuppressWarnings("unchecked")
    OutputTimeFn<? super W> typedOutputTimeFn = (OutputTimeFn<? super W>) outputTimeFn;

    return WindowingStrategy.of(
        getWindowFn(),
        getTrigger(), isTriggerSpecified(),
        getMode(), isModeSpecified(),
        getAllowedLateness(), isAllowedLatenessSpecified(),
        typedOutputTimeFn, true,
        getClosingBehavior());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("windowFn", getWindowFn())
        .add("allowedLateness", getAllowedLateness())
        .add("trigger", getTrigger())
        .add("accumulationMode", getMode())
        .add("outputTimeFn", getOutputTimeFn())
        .toString();
  }
}
