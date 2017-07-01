package br.com.mapreduce.mean;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

class MeanReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private DoubleWritable finalMean = new DoubleWritable();
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        String measurement = context.getConfiguration().get(MeanJob.CONF_NAME_MEASUREMENT);
        //if (key.equals( new Text(measurement))) {
            double sum = 0;
            double count = 0;
            for (DoubleWritable value : values) {
                sum += value.get();
                count += 1;
            }
            finalMean.set(sum/count);
            System.out.println("Media: " +sum/count);
            context.write(key, finalMean);
        //}
    }
}