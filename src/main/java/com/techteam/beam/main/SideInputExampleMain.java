package com.techteam.beam.main;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class SideInputExampleMain {
    public static void main(String[] args) {
        sideInputRun();
    }

    private static void sideInputRun() {
        Pipeline pipeline = Pipeline.create();

        String products = "/data/section1/products.csv";

        PCollection<String> productCollection =
                pipeline.apply(TextIO.read().from(products))
                        .apply("FilterHeader", Filter.by(line ->
                                !line.isEmpty() && !line.contains("ProductId, ProductName, ProductTypeId, Price"))
                        );

        PCollectionView<Double> averagePrice = productCollection
                .apply("Extract Price", FlatMapElements.into(TypeDescriptors.doubles())
                        .via((String line) ->
                                Collections.singletonList(Double.parseDouble(line.split(",")[3]))
                        ))
                .apply("Average Price", Combine.globally((SerializableFunction<Iterable<Double>, Double>) prices -> {
                    List<Double> collect = StreamSupport.stream(prices
                                    .spliterator(), false)
                            .collect(Collectors.toList());
                    double sum = collect.stream().mapToDouble(i -> i).sum();
                    return sum / collect.size();
                }).asSingletonView());

        productCollection.apply("Side Input", ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(ProcessContext processContext) {
                String strings = processContext.element();
                assert strings!= null;
                String[] splits = strings.split(",");
                double max = processContext.sideInput(averagePrice);
                double price = Double.parseDouble(splits[3].trim());

                //The product greater than average price
                if (price >= max) {
                    System.out.println("product info: " + String.join(",", splits[0], splits[1], splits[2], splits[3]));
                }
            }
        }).withSideInputs(averagePrice));

        pipeline.run().waitUntilFinish();
    }
}
