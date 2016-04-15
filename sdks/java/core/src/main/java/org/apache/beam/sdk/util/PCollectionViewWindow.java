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

import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollectionView;

import com.google.auto.value.AutoValue;
/**
 * A pair of a {@link PCollectionView} and a {@link BoundedWindow}, which can
 * be thought of as window "of" the view. This is a value class for use e.g.
 * as a compound cache key.
 *
 * @param <T> the type of the underlying PCollectionView
 */
@AutoValue
public abstract class PCollectionViewWindow<T> {

  public abstract PCollectionView<T> getView();
  public abstract BoundedWindow getWindow();

  public static <T> PCollectionViewWindow<T> of(PCollectionView<T> view, BoundedWindow window) {
    return new AutoValue_PCollectionViewWindow<>(view, window);
  }
}
