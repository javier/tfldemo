package com.teowaki;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;

public class InAndOut {

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();

        // Create the Pipeline object with the options we defined above.
        Pipeline p = Pipeline.create(options);

        //p.apply(TextIO.read().from("gs://whatever/*"))
        p.apply(TextIO.read().from("examples/86JourneyDataExtract29Nov2017-05Dec2017.csv"))
            .apply(TextIO.write().to("/tmp/just_out"));

        // Run the pipeline.
        p.run().waitUntilFinish();
    }

}
