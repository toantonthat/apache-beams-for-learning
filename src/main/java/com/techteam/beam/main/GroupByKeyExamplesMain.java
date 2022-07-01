package com.techteam.beam.main;

import org.apache.beam.sdk.Pipeline;

public class GroupByKeyExamplesMain {
    public static void main(String[] args) {
        runGroupByKeyBigQuery();
    }

    private static void runGroupByKeyBigQuery() {
        Pipeline p = Pipeline.create();



        p.run().waitUntilFinish();
    }
}
