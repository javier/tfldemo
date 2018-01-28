package com.teowaki;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.apache.beam.sdk.transforms.View;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class RecordsPerStartStationWithDiscardedAndStations {
    private static final Logger LOG = LoggerFactory.getLogger(JourneyRecord.class);
    private static Counter elementsCounter = Metrics.counter(RecordsPerStartStationWithDiscardedAndStations.class, "total elements");
    private static Counter validRecordsCounter = Metrics.counter(RecordsPerStartStationWithDiscardedAndStations.class, "elements added");
    private static Counter invalidRecordsCounter = Metrics.counter(RecordsPerStartStationWithDiscardedAndStations.class, "elements discarded");

    public static void main(String[] args) {


        RunnerOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(RunnerOptions.class);
        LOG.info("Starting!!! XXX");
        Pipeline p = Pipeline.create(options);

        final PCollectionView<Map<Integer, StationRecord>> stationRecordsView =
        p.apply("Read Stations CSV", TextIO.read().from(options.getStationsFileSource()))
        .apply( "Parse Stations", ParDo.of(new DoFn<String, KV<Integer, StationRecord>>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                StationRecord station = new StationRecord(c.element().toString());
                c.output(KV.of(station.stationId, station));
            }
        })).apply("To Side Input", View.<Integer, StationRecord>asMap());

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

            .apply("Enrich Journey with Station data", ParDo.of(new DoFn<KV<Integer,Long>, StationRecord>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                    Integer stationId = c.element().getKey();
                    Long count = c.element().getValue();
                    StationRecord station = c.sideInput(stationRecordsView).get(stationId);
                    if(station != null) {
                        StationRecord stationWithStats = new StationRecord(station);
                        stationWithStats.stats.put("count", new Double(count));
                        c.output(stationWithStats);
                    }
                }
            }).withSideInputs(stationRecordsView))

            .apply("Format", MapElements.via(new SimpleFunction<StationRecord, String>() {
                @Override
                public String apply(StationRecord input) {
                    return new StringBuffer(input.stationId.toString()).append(',')
                            .append(input.stationName).append(',')
                            .append(input.lat.toString()).append(',')
                            .append(input.lon.toString()).append(',')
                            .append(input.stats.get("count").toString())
                            .toString();
                }
            }))

            .apply("Total per Start Station File", TextIO.write().to(options.getOutputPath()));

        // Run the pipeline.
        p.run().waitUntilFinish();
    }

}
