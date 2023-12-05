/*
 * Copyright (C) 2023 Google Inc.
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

package com.example.dataflow;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;

public class MappingCountingAndFiltering {
  public interface Options extends PipelineOptions {}

  public static void main(String[] args) throws Exception {
    var options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    var pipeline = Pipeline.create(options);

    var data =
        pipeline
            .apply(
                "InputData",
                Create.of(
                    new Data(1, List.of(new Affinity(2, 0.1), new Affinity(3, 0.2))),
                    new Data(1, List.of(new Affinity(2, 0.1), new Affinity(5, 0.2))),
                    new Data(1, List.of(new Affinity(2, 0.5))),
                    new Data(3, List.of()),
                    new Data(2, List.of(new Affinity(1, 0.1)))))
            .apply("AddKeys", WithKeys.of(d -> UUID.randomUUID().toString()))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(Data.class)));

    var mapping =
        pipeline
            .apply("InputAliases", Create.of(new Mapping(1, "id1"), new Mapping(2, "id2")))
            .apply("KeyedAliases", WithKeys.of(map -> map.alias()))
            .setCoder(KvCoder.of(VarLongCoder.of(), SerializableCoder.of(Mapping.class)));

    var explodedData =
        data.apply(
                "ExplodeData",
                ParDo.of(
                    new DoFn<KV<String, Data>, KV<Long, DataExpectedValues>>() {

                      @ProcessElement
                      public void process(ProcessContext context) {
                        var data = context.element().getValue();
                        Stream.concat(
                                Stream.of(data.alias()),
                                data.affinities().stream().map(aff -> aff.withAlias()))
                            .forEach(
                                alias ->
                                    context.output(
                                        KV.of(
                                            alias,
                                            new DataExpectedValues(
                                                context.element().getKey(),
                                                data.countAliases(),
                                                0,
                                                Set.of()))));
                      }
                    }))
            .setCoder(
                KvCoder.of(VarLongCoder.of(), SerializableCoder.of(DataExpectedValues.class)));

    var mapTag = new TupleTag<Mapping>() {};
    var dataExpectationsTag = new TupleTag<DataExpectedValues>() {};

    var joinExplodedKeysAndAliases =
        KeyedPCollectionTuple.of(mapTag, mapping)
            .and(dataExpectationsTag, explodedData)
            .apply("JoinByAlias", CoGroupByKey.create());

    var deAliasableData =
        joinExplodedKeysAndAliases
            .apply(
                "FilterEmptyKeysAndReturnData",
                ParDo.of(
                    new DoFn<KV<Long, CoGbkResult>, DataExpectedValues>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        // filtering those that have present keys
                        if (c.element().getValue().getAll(mapTag).iterator().hasNext()) {
                          var mappings =
                              StreamSupport.stream(
                                      c.element().getValue().getAll(mapTag).spliterator(), false)
                                  .collect(Collectors.toSet());
                          c.element()
                              .getValue()
                              .getAll(dataExpectationsTag)
                              .forEach(
                                  data ->
                                      c.output(
                                          DataExpectedValues.includeMapping(
                                              data, mappings.size(), mappings)));
                        }
                      }
                    }))
            .apply("CombineCounting", Combine.globally(CombineCounting.create()))
            .apply("Flatten", Flatten.iterables())
            .apply("OnlyKeepThoseWhoMatchAliasCount", Filter.by(DataExpectedValues::suffice))
            .apply("ExtractDataKeys", WithKeys.of(DataExpectedValues::key))
            .setCoder(
                KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(DataExpectedValues.class)));

    var dataKeysTag = new TupleTag<DataExpectedValues>() {};
    var dataTag = new TupleTag<Data>() {};

    var joinedDataAndAliases =
        KeyedPCollectionTuple.of(dataKeysTag, deAliasableData)
            .and(dataTag, data)
            .apply("JoinDataWithAlias", CoGroupByKey.create());

    joinedDataAndAliases
        .apply(
            "CaptureResults",
            ParDo.of(
                new DoFn<KV<String, CoGbkResult>, Result>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    // filtering those that have present keys
                    if (c.element().getValue().getAll(dataKeysTag).iterator().hasNext()) {
                      var expectations =
                          c.element().getValue().getAll(dataKeysTag).iterator().next();
                      c.element()
                          .getValue()
                          .getAll(dataTag)
                          .forEach(
                              data -> c.output(new Result(data, expectations.mappedAliases())));
                    }
                  }
                }))
        .apply(
            "Print",
            ParDo.of(
                new DoFn<Result, Void>() {

                  @ProcessElement
                  public void process(ProcessContext context) {
                    System.out.println(context.element());
                  }
                }));

    pipeline.run().waitUntilFinish();
  }

  static class CombineCounting
      extends Combine.CombineFn<
          DataExpectedValues, AccumulatedDataToCheck, Iterable<DataExpectedValues>> {

    static CombineCounting create() {
      return new CombineCounting();
    }

    @Override
    public AccumulatedDataToCheck createAccumulator() {
      return AccumulatedDataToCheck.empty();
    }

    @Override
    public AccumulatedDataToCheck addInput(
        AccumulatedDataToCheck mutableAccumulator, DataExpectedValues dataKey) {
      return mutableAccumulator.accumulate(dataKey);
    }

    @Override
    public AccumulatedDataToCheck mergeAccumulators(Iterable<AccumulatedDataToCheck> accumulators) {
      return StreamSupport.stream(accumulators.spliterator(), false)
          .reduce(AccumulatedDataToCheck.empty(), (acc1, acc2) -> acc1.accumulate(acc2));
    }

    @Override
    public Iterable<DataExpectedValues> extractOutput(AccumulatedDataToCheck accumulator) {
      // need to be a list to be serializable
      return accumulator.accum().values().stream().toList();
    }
  }

  record Result(Data data, Set<Mapping> aliasMapping) implements Serializable {}

  record DataExpectedValues(
      String key, int expectedAliasCount, int accumulatedAliasCount, Set<Mapping> mappedAliases)
      implements Serializable {

    boolean suffice() {
      return expectedAliasCount == accumulatedAliasCount;
    }

    DataExpectedValues accumulate(DataExpectedValues other) {
      return includeMapping(this, other.accumulatedAliasCount(), other.mappedAliases());
    }

    static DataExpectedValues includeMapping(
        DataExpectedValues dataExpected, int accAliasCount, Iterable<Mapping> aliasMapping) {

      return new DataExpectedValues(
          dataExpected.key(),
          dataExpected.expectedAliasCount(),
          dataExpected.accumulatedAliasCount() + accAliasCount,
          Stream.concat(
                  dataExpected.mappedAliases().stream(),
                  StreamSupport.stream(aliasMapping.spliterator(), false))
              .collect(Collectors.toSet()));
    }
  }

  record AccumulatedDataToCheck(Map<String, DataExpectedValues> accum) implements Serializable {

    static AccumulatedDataToCheck empty() {
      return new AccumulatedDataToCheck(new HashMap<>());
    }

    public AccumulatedDataToCheck accumulate(DataExpectedValues data) {
      var newMap = new HashMap<>(this.accum());
      newMap.compute(
          data.key(),
          (key, value) -> Optional.ofNullable(value).map(v -> v.accumulate(data)).orElse(data));
      return new AccumulatedDataToCheck(newMap);
    }

    public AccumulatedDataToCheck accumulate(AccumulatedDataToCheck another) {
      return new AccumulatedDataToCheck(
          Stream.of(this.accum(), another.accum())
              .flatMap(map -> map.entrySet().stream())
              .collect(
                  Collectors.toMap(
                      Map.Entry::getKey, Map.Entry::getValue, (v1, v2) -> v1.accumulate(v2))));
    }
  }

  record Affinity(long withAlias, double affinity) implements Serializable {}

  record Data(long alias, List<Affinity> affinities) implements Serializable {
    public int countAliases() {
      return 1 + affinities().size();
    }
  }

  record Mapping(long alias, String realId) implements Serializable {}
}
