package com.techteam.beam.main;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.*;

import java.util.Objects;

public class GroupByKeyExamplesMain {
    public static void main(String[] args) {
        runGroupByKeyBigQuery();
    }

    private static void runGroupByKeyBigQuery() {
        String products = "/data/section1/products.csv";
        String productTypes = "/data/section1/product_types.csv";
        Pipeline pipeline = Pipeline.create();
        PCollection<KV<Integer, String>> productCollection =
                pipeline.apply(TextIO.read().from(products))
                        .apply("FilterHeader", Filter.by(line ->
                                !line.isEmpty() && !line.contains("ProductId, ProductName, ProductTypeId, Price")))
                                .apply(ParDo.of(new DoFn<String, KV<Integer, String>>() {
                                    @ProcessElement
                                    public void processElement(ProcessContext c) {
                                        String row = c.element();
                                        assert row != null;
                                        String[] splits = row.split(",");

                                        c.output(KV.of(Integer.valueOf(splits[0]), String.join(",",
                                                splits[0], splits[1], splits[2], splits[3])));
                                    }
                                }));

        PCollection<KV<Integer, String>> productTypeCollection =
                pipeline.apply(TextIO.read().from(productTypes))
                        .apply("FilterHeader", Filter.by(line ->
                                !line.isEmpty() && !line.contains("ProductTypeId, ProductType")))
                        .apply(ParDo.of(new DoFn<String, KV<Integer, String>>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                String row = c.element();
                                assert row != null;
                                String[] splits = row.split(",");
                                c.output(KV.of(Integer.valueOf(splits[0]), String.join(",",
                                        splits[0], splits[1])));
                            }
                        }));

        TupleTag<String> productTag = new TupleTag<>();
        TupleTag<String> productTypeTag = new TupleTag<>();

        PCollection<KV<Integer, CoGbkResult>> result = KeyedPCollectionTuple.of(productTag, productCollection)
                        .and(productTypeTag, productTypeCollection).apply(CoGroupByKey.create());

        PCollection<String> resultCollection = result
                .apply(ParDo.of(new ProcessJoinColumns()))
                ;

        resultCollection
                .apply(TextIO
                        .write().withoutSharding()
                        .to("result/group_products")
                        .withHeader("ProductTypeId, ProductId, ProductName, Price, ProductType")
                        .withSuffix(".csv"));

        pipeline.run().waitUntilFinish();
    }

    private static class ProcessJoinColumns extends DoFn<KV<Integer, CoGbkResult>, String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            KV<Integer, CoGbkResult> joinedStream = c.element();
            TupleTag product = joinedStream.getValue().getSchema().getTag(0);
            TupleTag productType = joinedStream.getValue().getSchema().getTag(1);
            String productRow = joinedStream.getValue().getOnly(product, 0) + "";
            String productTypeRow = joinedStream.getValue().getOnly(productType, 0) + "";
            c.output(String.join(",", joinedStream.getKey() + "",
                    productRow, productTypeRow
            ));
        }
    }
}
