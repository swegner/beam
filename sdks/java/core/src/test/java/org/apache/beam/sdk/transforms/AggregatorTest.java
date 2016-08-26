/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.beam.sdk.transforms;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.Iterables;
import java.io.Serializable;
import java.util.Collection;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.sdk.AggregatorRetrievalException;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.testing.RunnableOnService;
import org.apache.beam.sdk.testing.TestPipeline;

import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TimestampedValue;
import org.hamcrest.Matchers;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Unit tests to verify runner {@link Aggregator} support.
 */
public class AggregatorTest implements Serializable {
  static class DoFnWithAggregator<InputT, OutputT> extends DoFn<String, String> {
    static final AtomicLong UNIQUE_IDS = new AtomicLong();
    final Aggregator<InputT, OutputT> aggregator;
    final InputT val;

    DoFnWithAggregator(InputT val, Combine.CombineFn<InputT, ?, OutputT> combineFn) {
      this.val = val;
      this.aggregator = createAggregator("agg" + UNIQUE_IDS.incrementAndGet(), combineFn);
    }

    @DoFn.ProcessElement
    public void processElement(ProcessContext c) {
      aggregator.addValue(val);
      c.output(c.element());
    }
  }

  @Test
  @Category(RunnableOnService.class)
  public void testCombineFn() throws AggregatorRetrievalException {
    DoFnWithAggregator<Double, Double> fn = new DoFnWithAggregator<>(1.0, new Sum.SumDoubleFn());
    PipelineResult result = runPipelineWithTwoElements(ParDo.of(fn));

    Collection<Double> aggregatorValues = result.getAggregatorValues(fn.aggregator)
        .getValues();
    assertEquals(2.0, Iterables.getOnlyElement(aggregatorValues), 1e-6);
  }

  @Test
  @Category(RunnableOnService.class)
  public void testAnonymousCombineFn() throws AggregatorRetrievalException {
    DoFnWithAggregator<Double, Double> fn = new DoFnWithAggregator<>(1.0,
        new Combine.BinaryCombineDoubleFn() {
          @Override
          public double apply(double left, double right) {
            return left + right;
          }
          @Override
          public double identity() {
            return 0;
          }
        });
    PipelineResult result = runPipelineWithTwoElements(ParDo.of(fn));

    Collection<Double> aggregatorValues = result.getAggregatorValues(fn.aggregator)
        .getValues();
    assertEquals(2.0, Iterables.getOnlyElement(aggregatorValues), 1e-6);
  }

  @Test
  @Category(RunnableOnService.class)
  public void testSerializableFn() throws AggregatorRetrievalException {
    class DoFnWithSerializableFnAggregator extends DoFn<String, String> {
      final Aggregator<Double, Double> aggregator;
      DoFnWithSerializableFnAggregator(SerializableFunction<Iterable<Double>, Double> fn) {
        aggregator = createAggregator("serializableAgg", fn);
      }

      @DoFn.ProcessElement
      public void processElement(ProcessContext c) {
        aggregator.addValue(1.0);
      }
    }
    DoFnWithSerializableFnAggregator fn = new DoFnWithSerializableFnAggregator(
        new SerializableFunction<Iterable<Double>, Double>() {
          @Override
          public Double apply(Iterable<Double> input) {
            Double total = 0.0;
            for (Double element : input) {
              total += element;
            }
            return total;
          }
        });

    PipelineResult result = runPipelineWithTwoElements(ParDo.of(fn));

    Collection<Double> aggregatorValues = result.getAggregatorValues(fn.aggregator)
        .getValues();
    assertEquals(2.0, Iterables.getOnlyElement(aggregatorValues), 1e-6);
  }

  @Test
  @Category(RunnableOnService.class)
  public void testStringAggregator() throws AggregatorRetrievalException {
    DoFnWithAggregator<TimestampedValue<String>, String> fn = new DoFnWithAggregator<>(
        TimestampedValue.of("foo", Instant.now()), new Latest.LatestFn<String>());
    PipelineResult result = runPipelineWithTwoElements(ParDo.of(fn));

    Collection<String> aggregatorValues = result.getAggregatorValues(fn.aggregator).getValues();
    assertEquals("foo", Iterables.getOnlyElement(aggregatorValues));
  }

  @Test
  @Category(RunnableOnService.class)
  public void testMultipleAggregators() throws AggregatorRetrievalException {
    final DoFnWithAggregator<Double, Double> fn1 = new DoFnWithAggregator<>(1.0,
        new Sum.SumDoubleFn());
    final DoFnWithAggregator<Double, Double> fn2 = new DoFnWithAggregator<>(4.0,
        new Mean.MeanFn<Double>());

    PTransform<PCollection<? extends String>, PCollection<String>> parallelTransform =
        new PTransform<PCollection<? extends String>, PCollection<String>>() {
      @Override
      public PCollection<String> apply(PCollection<? extends String> input) {
        PCollectionList<String> pcs = PCollectionList
            .of(input.apply("s1", ParDo.of(fn1)))
            .and(input.apply("s2", ParDo.of(fn2)));

        return pcs.apply(Flatten.<String>pCollections());
      }
    };

    PipelineResult result = runPipelineWithTwoElements(parallelTransform);
    Collection<Double> agg1Values = result.getAggregatorValues(fn1.aggregator).getValues();
    Collection<Double> agg2Values = result.getAggregatorValues(fn2.aggregator).getValues();

    assertEquals(2.0, Iterables.getOnlyElement(agg1Values), 1e-6);
    assertEquals(4.0, Iterables.getOnlyElement(agg2Values), 1e-6);
  }

  @Test
  public void testAggregatorUsedInMultipleSteps() throws AggregatorRetrievalException {
    final DoFnWithAggregator<Double, Double> fn = new DoFnWithAggregator<>(
        1.0, new Sum.SumDoubleFn());

    PTransform<PCollection<? extends String>, PCollection<String>> applyDoFnTwice =
        new PTransform<PCollection<? extends String>, PCollection<String>>() {
          @Override
          public PCollection<String> apply(PCollection<? extends String> input) {
            return input
                .apply("s1", ParDo.of(fn))
                .apply("s2", ParDo.of(fn));
          }
        };

    PipelineResult result = runPipelineWithTwoElements(applyDoFnTwice);
    Map<String, Double> valuesMap = result.getAggregatorValues(fn.aggregator)
        .getValuesAtSteps();

    assertThat(valuesMap.entrySet(), hasSize(2));
    assertThat(valuesMap.values(), Matchers.everyItem(is(2.0)));
  }

  private PipelineResult runPipelineWithTwoElements(
      PTransform<PCollection<? extends String>, PCollection<String>> transform) {
    TestPipeline p = TestPipeline.create();
    p
        .apply(Create.of("foo", "bar"))
        .apply(transform);

    return p.run();
  }
}