package com.teowaki;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class FilterColumns{
    private static final Logger LOG = LoggerFactory.getLogger(JourneyRecord.class);

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();

        LOG.info("Starting!!! XXX");
        Pipeline p = Pipeline.create(options);
       // CoderRegistry cr = p.getCoderRegistry();
        //cr.registerCoderForClass(JourneyRecord.class, SerializableCoder.class);

        p.apply(TextIO.read().from("examples/20170512.csv"))
            .apply("RemoveHeadersAndEmptyLines", ParDo.of(new DoFn<String, String>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                    String csv = c.element().toString();
                    if (!csv.isEmpty() && !csv.startsWith("Rental") ) c.output(c.element());
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

            .apply("toString", ParDo.of(new DoFn<JourneyRecord, String>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                    c.output(c.element().toString());
                }
            }))



                .apply(TextIO.write().to("/tmp/just_out"));

        // Run the pipeline.
        p.run().waitUntilFinish();
    }

}
