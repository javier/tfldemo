package com.teowaki;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecordsPerStartStation{
    private static final Logger LOG = LoggerFactory.getLogger(JourneyRecord.class);
    private static Counter elementsCounter = Metrics.counter(RecordsPerStartStation.class, "elements added");

    public static void main(String[] args) {
        RunnerOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(RunnerOptions.class);

        LOG.info("Starting!!! XXX");
        Pipeline p = Pipeline.create(options);



        p.apply(TextIO.read().from(options.getFileSource()))
            .apply("RemoveHdrsNEmptyLines", ParDo.of(new DoFn<String, String>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                    String csv = c.element().toString();
                    if (!csv.isEmpty() && !csv.startsWith("Rental") ) {
                        elementsCounter.inc();
                        c.output(c.element());
                    }
                }
            }))

            .apply("ParseCSV", ParDo.of(new DoFn<String, JourneyRecord>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                    String csv = c.element().toString();
                        try {
                            c.output(new JourneyRecord(csv));
                        } catch (Exception e) {
                            // XXX we are losing data here
                            e.printStackTrace();
                        }
                }
            }))

            .apply("StartStationAsKey", ParDo.of(new DoFn<JourneyRecord, KV<Integer, Integer>>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                    c.output(KV.of(c.element().startStationId, 1));
                }
            }))

            .apply(Count.<Integer, Integer>perKey())

            .apply("Format", MapElements.via(new SimpleFunction<KV<Integer,Long>, String>() {
                @Override
                public String apply(KV<Integer, Long> input) {
                    return new StringBuffer(input.getKey().toString()).append(',').append(input.getValue().toString()).toString();
                }
            }))

                .apply(TextIO.write().to(options.getOutputPath()));

        // Run the pipeline.
        p.run().waitUntilFinish();
    }

}
