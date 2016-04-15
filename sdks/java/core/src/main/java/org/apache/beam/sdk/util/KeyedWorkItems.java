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

import org.apache.beam.sdk.util.TimerInternals.TimerData;

import com.google.auto.value.AutoValue;

import java.util.Collections;

/**
 * Static utility methods that provide {@link KeyedWorkItem} implementations.
 */
public class KeyedWorkItems {
  /**
   * Returns an implementation of {@link KeyedWorkItem} that wraps around an elements iterable.
   *
   * @param <K> the key type
   * @param <ElemT> the element type
   */
  public static <K, ElemT> KeyedWorkItem<K, ElemT> elementsWorkItem(
      K key, Iterable<WindowedValue<ElemT>> elementsIterable) {
    return ComposedKeyedWorkItem.of(key, Collections.<TimerData>emptyList(), elementsIterable);
  }

  /**
   * Returns an implementation of {@link KeyedWorkItem} that wraps around an timers iterable.
   *
   * @param <K> the key type
   * @param <ElemT> the element type
   */
  public static <K, ElemT> KeyedWorkItem<K, ElemT> timersWorkItem(
      K key, Iterable<TimerData> timersIterable) {
    return ComposedKeyedWorkItem.of(
        key, timersIterable, Collections.<WindowedValue<ElemT>>emptyList());
  }

  /**
   * Returns an implementation of {@link KeyedWorkItem} that wraps around
   * an timers iterable and an elements iterable.
   *
   * @param <K> the key type
   * @param <ElemT> the element type
   */
  public static <K, ElemT> KeyedWorkItem<K, ElemT> workItem(
      K key, Iterable<TimerData> timersIterable, Iterable<WindowedValue<ElemT>> elementsIterable) {
    return ComposedKeyedWorkItem.of(key, timersIterable, elementsIterable);
  }

  /**
   * A {@link KeyedWorkItem} composed of an underlying key, {@link TimerData} iterable, and element
   * iterable.
   */
  @AutoValue
  public abstract static class ComposedKeyedWorkItem<K, ElemT> implements KeyedWorkItem<K, ElemT> {

    static <K, ElemT> ComposedKeyedWorkItem<K, ElemT> of(
        K key, Iterable<TimerData> timers, Iterable<WindowedValue<ElemT>> elements) {
      return new AutoValue_KeyedWorkItems_ComposedKeyedWorkItem<>(key, timers, elements);
    }
  }
}
