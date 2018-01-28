package com.teowaki;

import com.teowaki.utils.StringUtil;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;

@DefaultCoder(AvroCoder.class)
public class StationRecord {
    private static final Logger LOG = LoggerFactory.getLogger(StationRecord.class);

    @Nullable
    public String stationName;
    @Nullable
    public Double lat, lon;
    @Nullable
    public Integer stationId;
    @Nullable
    public HashMap<String, Double> stats;

    public StationRecord() {
        // for Avro
    }

    public StationRecord(StationRecord original) {
        this.stationId = new Integer(original.stationId);
        this.stationName = new String(original.stationName);
        this.lat = new Double(original.lat);
        this.lon = new Double(original.lon);
        this.stats = new HashMap<>(original.stats);
    }

    public StationRecord(String csv) {
        List<String> inputs = StringUtil.parseCsvLine(csv, ',','"', '\\',false);

        LOG.debug("XXX raw string:" + csv );
        try {
        this.stationId = parseId(inputs.get(0));
        this.stationName =inputs.get(1);
        this.lat = Double.parseDouble(inputs.get(2));
        this.lon = Double.parseDouble(inputs.get(3));
        this.stats = new HashMap<String, Double>();
        } catch (ArrayIndexOutOfBoundsException e) {
            LOG.error("XXX input:" + inputs.get(0));
        }
    }

    @Override
    public String toString() {
        return new StringBuffer(stationId.toString()).append(',')
                .append(stationName).append(',')
                .append(lat.toString()).append(',')
                .append(lon.toString()).toString();
    }

    private Integer parseId(String string_id) throws ArrayIndexOutOfBoundsException{
        return Integer.parseInt(string_id.split("_")[1]);
    }
}
