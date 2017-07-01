package br.com.mapreduce.stationgrep;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

class StationMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    @Override
    protected void map(LongWritable inputKey, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
        if (stringTokenizer.countTokens() > 0) {
            String token = stringTokenizer.nextToken();
            if (token.charAt(0) == 'S') {
                return;
            }

            long outputKey = Long.parseLong(token);
            context.write(new LongWritable(outputKey), value);
            //System.out.println("<" + outputKey + ", " + value.toString().substring(0, 100) + ">");
        }
    }
}
