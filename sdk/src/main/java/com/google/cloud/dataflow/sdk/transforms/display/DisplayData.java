/*
 * Copyright (C) 2016 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.transforms.display;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Static display metadata associated with a pipeline component. Display data is useful for
 * pipeline runner UIs and diagnostic dashboards to display details about {@link PTransform PTransforms}
 * that make up a pipeline.
 *
 * <p>Components specify their display data by implementing the {@link HasDisplayData}
 * interface.
 */
public class DisplayData {
  private static final DisplayData EMPTY = new DisplayData(Maps.<Identifier, Item<?>>newHashMap());
  private final ImmutableMap<Identifier, Item<?>> entries;

  private DisplayData(Map<Identifier, Item<?>> entries) {
    this.entries = ImmutableMap.copyOf(entries);
  }

  /**
   * Default empty {@link DisplayData} instance.
   */
  public static DisplayData none() {
    return EMPTY;
  }

  /**
   * Collect the {@link DisplayData} from a component. This will traverse all subcomponents
   * specified via {@link Builder#include} in the given component.
   */
  public static DisplayData from(HasDisplayData component) {
    checkNotNull(component);
    return InternalBuilder.forRoot(component).build();
  }

  public Collection<Item<?>> items() {
    return entries.values();
  }

  public Map<Identifier, Item<?>> asMap() {
    return entries;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    boolean isFirstLine = true;
    for (Map.Entry<Identifier, Item<?>> entry : entries.entrySet()) {
      if (isFirstLine) {
        isFirstLine = false;
      } else {
        builder.append("\n");
      }

      builder.append(entry);
    }

    return builder.toString();
  }

  /**
   * Utility to build up display metadata from a component and its included
   * subcomponents.
   */
  public interface Builder {
    /**
     * Include display metadata from the specified subcomponent. For example, a {@link ParDo}
     * transform includes display metadata from the encapsulated {@link DoFn}.
     *
     * @return A builder instance to continue to build in a fluent-style.
     */
    Builder include(HasDisplayData subComponent);

    /**
     * Register the given string display metadata. The metadata item will be registered with type
     * {@link Type#STRING}, and is identified by the specified key and namespace from the current
     * transform or component.
     */
    ItemBuilder add(String key, String value);

    /**
     * Register the given numeric display metadata. The metadata item will be registered with type
     * {@link Type#INTEGER}, and is identified by the specified key and namespace from the current
     * transform or component.
     */
    ItemBuilder add(String key, long value);

    /**
     * Register the given floating point display metadata. The metadata item will be registered with
     * type {@link Type#FLOAT}, and is identified by the specified key and namespace from the
     * current transform or component.
     */
    ItemBuilder add(String key, double value);

    /**
     * Register the given timestamp display metadata. The metadata item will be registered with type
     * {@link Type#TIMESTAMP}, and is identified by the specified key and namespace from the current
     * transform or component.
     */
    ItemBuilder add(String key, Instant value);

    /**
     * Register the given duration display metadata. The metadata item will be registered with type
     * {@link Type#DURATION}, and is identified by the specified key and namespace from the current
     * transform or component.
     */
    ItemBuilder add(String key, Duration value);

    /**
     * Register the given class display metadata. The metadata item will be registered with type
     * {@link Type#JAVA_CLASS}, and is identified by the specified key and namespace from the
     * current transform or component.
     */
    ItemBuilder add(String key, Class<?> value);
  }

  /**
   * Utility to append optional fields to display metadata, or register additional display metadata
   * items.
   */
  public interface ItemBuilder extends Builder {
    /**
     * Add a human-readable label to describe the previously-registered metadata field.
     * A label is optional; if unspecified, UIs should display the metadata key to identify the
     * display item.
     */
    ItemBuilder withLabel(String label);

    /**
     * Add a link URL to the previously-registered display metadata. A link URL is optional and
     * can be provided to point the reader to additional details about the metadata.
     */
    ItemBuilder withLinkUrl(String url);
  }

  /**
   * A display metadata item. DisplayData items are registered via {@link Builder#add} within
   * {@link HasDisplayData#populateDisplayData} implementations. Each metadata item is uniquely
   * identified by the specified key and namespace generated from the registering component's
   * class name.
   */
  public static class Item<T> {
    private final String key;
    private final String ns;
    private final Type type;
    private final T value;
    private final String label;
    private final String url;

    private static <T> Item<T> create(String namespace, String key, Type type, T value) {
      return new Item<>(namespace, key, type, value, null, null);
    }

    private Item(String namespace, String key, Type type, T value, String url, String label) {
      this.ns = namespace;
      this.key = key;
      this.type = type;
      this.value = value;
      this.url = url;
      this.label = label;
    }

    public String getNamespace() {
      return ns;
    }

    public String getKey() {
      return key;
    }
    /**
     * Retrieve the {@link Type} of display metadata. All metadata conforms to a predefined set of
     * allowed types.
     */
    public Type getType() {
      return type;
    }

    /**
     * Retrieve the value of the metadata item.
     */
    public T getValue() {
      return value;
    }

    /**
     * Retrieve the optional label for an item. The label is a human-readable description of what
     * the metadata represents. UIs may choose to display the label instead of the item key.
     *
     * </p>If no label was specified, this will return {@code null}.
     */
    public String getLabel() {
      return label;
    }

    /**
     * Retrieve the optional link URL for an item. The URL points to an address where the reader
     * can find additional context for the display metadata.
     *
     * </p>If no URL was specified, this will return {@code null}.
     */
    public String getUrl() {
      return url;
    }

    /**
     * Retrieve a serialized version of the display item value suitable for use as JSON.
     */
    public String getValueString() {
      return type.serializeJsonString(value);
    }

    @Override
    public String toString() {
      return getValueString();
    }

    private Item<T> withLabel(String label) {
      return new Item<>(this.ns, this.key, this.type, this.value, this.url, label);
    }

    private Item<T> withUrl(String url) {
      return new Item<>(this.ns, this.key, this.type, this.value, url, this.label);
    }
  }

  /**
   * Unique identifier for a display metadata item within a component.
   * Identifiers are composed of the key they are registered with and a namespace generated from
   * the class of the component which registered the item.
   *
   * <p>Display metadata registered with the same key from different components will have different
   * namespaces and thus will both be represented in the composed {@link DisplayData}. If a
   * single component registers multiple metadata items with the same key, only the most recent
   * item will be retained; previous versions are discarded.
   */
  public static class Identifier {
    private final String ns;
    private final String key;

    static Identifier of(Class<?> namespace, String key) {
      return new Identifier(namespace.getName(), key);
    }

    private Identifier(String ns, String key) {
      this.ns = ns;
      this.key = key;
    }

    public String getNamespace() {
      return ns;
    }

    public String getKey() {
      return key;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof Identifier) {
        Identifier that = (Identifier) obj;
        return this.ns.equals(that.ns)
            && this.key.equals(that.key);
      }

      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hash(ns, key);
    }

    @Override
    public String toString() {
      return String.format("%s:%s", ns, key);
    }
  }

  /**
   * Display metadata type.
   */
  enum Type {
    STRING {
      @Override
      String serializeJsonString(Object value) {
        return (String) value;
      }
    },
    INTEGER {
      @Override
      String serializeJsonString(Object value) {
        return Long.toString((long) value);
      }
    },
    FLOAT {
      @Override
      String serializeJsonString(Object value) {
        return Double.toString((Double) value);
      }
    },
    TIMESTAMP() {
      @Override
      String serializeJsonString(Object value) {
        return Long.toString(((Instant) value).getMillis());
      }
    },
    DURATION {
      @Override
      String serializeJsonString(Object value) {
        return Long.toString(((Duration) value).getMillis());
      }
    },
    JAVA_CLASS {
      @Override
      String serializeJsonString(Object value) {
        Class<?> clazz = (Class<?>) value;
        Map<String, String> jsonMap = Maps.newHashMap();
        jsonMap.put("name", clazz.getName());
        jsonMap.put("simpleName", clazz.getSimpleName());
        return toJson(jsonMap);
      }
    };

    /**
     * Serialize the display metadata value into a String which can be submitted as JSON
     * to a UI or diagnostic dashboard.
     *
     * <p>Internal-only. Value objects can be safely cast to the expected Java type.
     */
    abstract String serializeJsonString(Object value);

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    /**
     * Serialize the given object as JSON.
     */
    private static String toJson(Object obj) {
      try {
        return JSON_MAPPER.writeValueAsString(obj);
      } catch (IOException ex) {
        throw Throwables.propagate(ex);
      }
    }
  }

  private static class InternalBuilder implements ItemBuilder {
    private final Map<Identifier, Item<?>> entries;
    private final Set<Object> visited;

    private Class<?> latestNs;
    private Item<?> latestItem;
    private Identifier latestIdentifier;

    private InternalBuilder() {
      this.entries = Maps.newHashMap();
      this.visited = Sets.newIdentityHashSet();
    }

    private static InternalBuilder forRoot(HasDisplayData instance) {
      InternalBuilder builder = new InternalBuilder();
      builder.include(instance);
      return builder;
    }

    @Override
    public Builder include(HasDisplayData subComponent) {
      checkNotNull(subComponent);
      boolean newComponent = visited.add(subComponent);
      if (newComponent) {
        Class prevNs = this.latestNs;
        this.latestNs = subComponent.getClass();
        subComponent.populateDisplayData(this);
        this.latestNs = prevNs;
      }

      return this;
    }

    @Override
    public ItemBuilder add(String key, String value) {
      checkNotNull(value);
      return addItem(key, Type.STRING, value);
    }

    @Override
    public ItemBuilder add(String key, long value) {
      return addItem(key, Type.INTEGER, value);
    }

    @Override
    public ItemBuilder add(String key, double value) {
      return addItem(key, Type.FLOAT, value);
    }

    @Override
    public ItemBuilder add(String key, Instant value) {
      checkNotNull(value);
      return addItem(key, Type.TIMESTAMP, value);
    }

    @Override
    public ItemBuilder add(String key, Duration value) {
      checkNotNull(value);
      return addItem(key, Type.DURATION, value);
    }

    @Override
    public ItemBuilder add(String key, Class<?> value) {
      checkNotNull(value);
      return addItem(key, Type.JAVA_CLASS, value);
    }

    private <T> ItemBuilder addItem(String key, Type type, T value) {
      checkNotNull(key);
      checkArgument(!key.isEmpty());

      Identifier id = Identifier.of(latestNs, key);
      if (entries.containsKey(id)) {
        throw new IllegalArgumentException("DisplayData key already exists. All display data "
          + "for a component must be registered with a unique key.\nKey: " + id);
      }
      Item<T> item = Item.create(id.getNamespace(), key, type, value);
      entries.put(id, item);

      latestItem = item;
      latestIdentifier = id;

      return this;
    }

    @Override
    public ItemBuilder withLabel(String label) {
      Item<?> newItem = latestItem.withLabel(label);
      entries.put(latestIdentifier, newItem);

      latestItem = newItem;
      return this;
    }

    @Override
    public ItemBuilder withLinkUrl(String url) {
      Item<?> newItem = latestItem.withUrl(url);
      entries.put(latestIdentifier, newItem);

      latestItem = newItem;
      return this;
    }

    private DisplayData build() {
      return new DisplayData(this.entries);
    }
  }
}
