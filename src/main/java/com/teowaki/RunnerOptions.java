package com.teowaki;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface RunnerOptions extends PipelineOptions {
    @Description("Path of the file to read from (local or gs://)")
    @Default.String("./examples/minimal.csv")
    String getFileSource();
    void setFileSource(String value);

    @Description("Path of the stations file to read from (local or gs://)")
    @Default.String("./examples/bikeStations.csv")
    String getStationsFileSource();
    void setStationsFileSource(String value);

    @Description("Output path (local or gs://)")
    @Default.String("/tmp/out.csv")
    String getOutputPath();
    void setOutputPath(String value);

    @Description("Invalid Records Output path (local or gs://)")
    @Default.String("/tmp/invalid.out.csv")
    String getInvalidRecordsOutputPath();
    void setInvalidRecordsOutputPath(String value);

}

