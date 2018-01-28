package com.teowaki;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.teowaki.utils.StringUtil;


import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)
public class JourneyRecord {
    private static final Logger LOG = LoggerFactory.getLogger(JourneyRecord.class);
    private static final DateFormat DATE_FORMAT = new SimpleDateFormat("dd/MM/yyyy HH:mm");

    @Nullable
    public String rentalId, startStationName, endStationName;
    @Nullable
    public Integer duration, bikeId, startStationId, endStationId;
    @Nullable
    public Long startDate, endDate;


    public JourneyRecord() {
        // for Avro
    }

    public JourneyRecord(String csv) throws ParseException {
        List<String> inputs = StringUtil.parseCsvLine(csv, ',','"', '\\',false);

        LOG.debug("XXX raw string:" + csv );
        this.rentalId = inputs.get(0);
        this.duration = Integer.parseInt(inputs.get(1));
        this.bikeId = Integer.parseInt(inputs.get(2));
        try {
            this.endDate = DATE_FORMAT.parse(inputs.get(3)).getTime();;
        } catch (ParseException e) {
            LOG.error("XXX input:" + inputs.get(3));
            throw(e);
        }
        this.endStationId = Integer.parseInt(inputs.get(4));
        this.endStationName = inputs.get(5);
        try {
            this.startDate = DATE_FORMAT.parse(inputs.get(6)).getTime();
        } catch (ParseException e) {
            LOG.error("XXX input:" + inputs.get(6));
            throw(e);
        }
        this.startStationId = Integer.parseInt(inputs.get(7));
        this.startStationName = inputs.get(8);
    }

    @Override
    public String toString() {
        return new StringBuffer(rentalId).append(',')
                .append(bikeId).append(',')
                .append(startStationId).append(',')
                .append(DATE_FORMAT.format(this.startDate)).append(',')
                .append(DATE_FORMAT.format(this.endDate)).append(',')
                .append(endStationId)
                .append(',')
                .append(duration).toString();
    }


}
