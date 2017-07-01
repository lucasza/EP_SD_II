package br.com.mapreduce.dategrep;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class DateMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    @Override
    protected void map(LongWritable inputKey, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\\s+");
        if (tokens.length > 2) {
            String token = tokens[0];
            if (token.charAt(0) == 'S') {
                return;
            }
            token = tokens[2];

            long outputKey = Long.parseLong(token);
            context.write(new LongWritable(outputKey), value);
            //System.out.println("<" + outputKey + ", " + value.toString().substring(0, 100) + ">");
        }
    }
}
