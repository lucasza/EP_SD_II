package br.com.mapreduce.dategrep;

import br.com.mapreduce.GrepReducer;
import org.apache.hadoop.io.LongWritable;

class DateReducer extends GrepReducer {
    protected boolean isInsideGrep(Context context, LongWritable key) {
        int begin = context.getConfiguration().getInt(DateGrepJob.CONF_NAME_DATE_BEGIN, -1);
        int end = context.getConfiguration().getInt(DateGrepJob.CONF_NAME_DATE_END, -1);
        return key.compareTo( new LongWritable( begin ) ) >= 0 &&  key.compareTo( new LongWritable( end ) ) <= 0;
    }
}
