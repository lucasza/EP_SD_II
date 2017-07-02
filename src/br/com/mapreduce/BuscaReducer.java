package br.com.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public abstract class BuscaReducer extends Reducer<LongWritable, Text, Text, Text>{
    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        if ( isInsideGrep(context, key) ) {
            for (Text text : values) {
                context.write(text, new Text());
                System.out.println(text);
            }
        }
    }
    protected abstract boolean isInsideGrep(Context context, LongWritable key);
}
