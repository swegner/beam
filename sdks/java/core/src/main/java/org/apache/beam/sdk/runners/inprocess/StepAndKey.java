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
package org.apache.beam.sdk.runners.inprocess;

import org.apache.beam.sdk.transforms.AppliedPTransform;

import com.google.auto.value.AutoValue;

/**
 * A (Step, Key) pair. This is useful as a map key or cache key for things that are available
 * per-step in a keyed manner (e.g. State).
 */
@AutoValue
abstract class StepAndKey {
  abstract AppliedPTransform<?, ?, ?> getStep();
  abstract Object getKey();

  /**
   * Create a new {@link StepAndKey} with the provided step and key.
   */
  public static StepAndKey of(AppliedPTransform<?, ?, ?> step, Object key) {
    return new AutoValue_StepAndKey(step, key);
  }
}
