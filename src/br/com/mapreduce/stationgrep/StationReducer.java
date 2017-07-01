package br.com.mapreduce.stationgrep;

import br.com.mapreduce.GrepReducer;
import org.apache.hadoop.io.LongWritable;

class StationReducer extends GrepReducer {
    protected boolean isInsideGrep(Context context, LongWritable key) {
        int station = context.getConfiguration().getInt(StationGrepJob.CONF_NAME_STATION, -1);
        return key.equals( new LongWritable(station));
    }
}
