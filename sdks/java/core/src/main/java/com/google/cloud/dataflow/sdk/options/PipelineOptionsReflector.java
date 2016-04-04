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
package com.google.cloud.dataflow.sdk.options;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.util.common.ReflectHelpers;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.beans.Introspector;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Set;

/**
 * Utilities to reflect over {@link PipelineOptions}.
 */
class PipelineOptionsReflector {
  private PipelineOptionsReflector() {}

  /**
   * Retrieve the full set of pipeline option properties visible within the type hierarchy closure
   * for the set of input interfaces. An option is "visible" if:
   *
   * <ul>
   *   <li>The option is defined within the interface hierarchy closure of the input
   *   {@link PipelineOptions}.</li>
   *   <li>The defining interface is not marked {@link Hidden}.</li>
   * </ul>
   */
  static Set<Property> collectVisibleProperties(
      Class<? extends PipelineOptions> optionsInterface) {
    Iterable<Method> methods = ReflectHelpers.getClosureOfMethodsOnInterface(optionsInterface);
    ImmutableSet.Builder<Property> setBulder = ImmutableSet.builder();
    for (Method method : methods) {
      String methodName = method.getName();
      if ((!methodName.startsWith("get")
          && !methodName.startsWith("is"))
          || method.getParameterTypes().length != 0
          || method.getReturnType() == void.class) {
        continue;
      }
      String name = Introspector.decapitalize(
          methodName.startsWith("is") ? methodName.substring(2) : methodName.substring(3));
      Class<?> declaringClass = method.getDeclaringClass();

      if (!PipelineOptions.class.isAssignableFrom(declaringClass)) {
        continue;
      }

      if (declaringClass.isAnnotationPresent(Hidden.class)) {
        continue;
      }

      setBulder.add(Property.of((Class<? extends PipelineOptions>)declaringClass, name, method));
    }

    return setBulder.build();
  }

  /**
   * Retrieve the full set of pipeline option properties visible within the type hierarchy closure
   * for the set of input interfaces. An option is "visible" if:
   *
   * <ul>
   *   <li>The option is defined within the interface hierarchy closure of the input
   *   {@link PipelineOptions}.</li>
   *   <li>The defining interface is not marked {@link Hidden}.</li>
   * </ul>
   */
  static Set<Property> collectVisibleProperties(
      Collection<Class<? extends PipelineOptions>> optionsInterfaces) {
    ImmutableSet.Builder<Property> setBulder = ImmutableSet.builder();
    for (Class<? extends PipelineOptions> optionsInterface : optionsInterfaces) {
      setBulder.addAll(collectVisibleProperties(optionsInterface));
    }

    return setBulder.build();
  }

  /**
   * A property defined in a {@link PipelineOptions} interface.
   */
  static class Property {
    private final Class<? extends PipelineOptions> clazz;
    private final String name;
    private final Method getter;

    static Property of(Class<? extends PipelineOptions> clazz, String name, Method getter) {
      return new Property(clazz, name, getter);
    }

    private Property(Class<? extends PipelineOptions> clazz, String name, Method getter) {
      this.clazz = clazz;
      this.name = name;
      this.getter = getter;
    }

    Class<? extends PipelineOptions> definingClass() {
      return clazz;
    }

    String name() {
      return name;
    }

    Method getterMethod() {
      return getter;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("definingClass", definingClass())
          .add("name", name())
          .add("getterMethod", getterMethod())
          .toString();
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(definingClass(), name(), getterMethod());
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof Property)) {
        return false;
      }

      Property that = (Property) obj;
      return Objects.equal(this.definingClass(), that.definingClass())
          && Objects.equal(this.name(), that.name())
          && Objects.equal(this.getterMethod(), that.getterMethod());
    }
  }
}
