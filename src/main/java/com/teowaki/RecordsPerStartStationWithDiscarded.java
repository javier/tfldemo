package com.teowaki;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecordsPerStartStationWithDiscarded {
    private static final Logger LOG = LoggerFactory.getLogger(JourneyRecord.class);
    private static Counter elementsCounter = Metrics.counter(RecordsPerStartStationWithDiscarded.class, "total elements");
    private static Counter validRecordsCounter = Metrics.counter(RecordsPerStartStationWithDiscarded.class, "elements added");
    private static Counter invalidRecordsCounter = Metrics.counter(RecordsPerStartStationWithDiscarded.class, "elements discarded");

    public static void main(String[] args) {
        RunnerOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(RunnerOptions.class);

        LOG.info("Starting!!! XXX");
        Pipeline p = Pipeline.create(options);

        final TupleTag<JourneyRecord> validLinesTag = new TupleTag<JourneyRecord>(){};
        final TupleTag<String> invalidLinesTag = new TupleTag<String>(){};


        PCollectionTuple validAndInvalidLines =
        p.apply("Read From CSV", TextIO.read().from(options.getFileSource()))
            .apply("Remove Headers and Empty Lines", ParDo.of(new DoFn<String, String>() {
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
                            validRecordsCounter.inc();
                            c.output(new JourneyRecord(csv));
                        } catch (Exception e) {
                            // XXX we are losing data here
                            invalidRecordsCounter.inc();
                            c.output(invalidLinesTag, c.element());
                            e.printStackTrace();
                        }
                }
            }).withOutputTags(validLinesTag, TupleTagList.of(invalidLinesTag)));


            validAndInvalidLines.get(invalidLinesTag)
            .apply("Invalid outputs", TextIO.write().to(options.getInvalidRecordsOutputPath()));

            validAndInvalidLines.get(validLinesTag)
            .apply("StartStationAsKey", ParDo.of(new DoFn<JourneyRecord, KV<Integer, Integer>>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                    c.output(KV.of(c.element().startStationId, 1));
                }
            }))

            .apply("Total per Start Station Count", Count.<Integer, Integer>perKey())

            .apply("Format", MapElements.via(new SimpleFunction<KV<Integer,Long>, String>() {
                @Override
                public String apply(KV<Integer, Long> input) {
                    return new StringBuffer(input.getKey().toString()).append(',').append(input.getValue().toString()).toString();
                }
            }))

            .apply("Total per Start Station File", TextIO.write().to(options.getOutputPath()));

        // Run the pipeline.
        p.run().waitUntilFinish();
    }

}
