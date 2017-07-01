package br.com.mapreduce.stddeviation;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

class StdDeviationReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private DoubleWritable standardDeviation = new DoubleWritable();
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        String measurement = context.getConfiguration().get(JobDesvioPadrao.CONF_NAME_MEASUREMENT);
        if (key.equals( new Text(measurement))) {
            List<DoubleWritable> backupList = new LinkedList<DoubleWritable>();
            double sum = 0;
            double N = 0;
            for (DoubleWritable value : values) {
                sum += value.get();
                N += 1;
                backupList.add(value);
            }
            double mean = sum/N;
            double deviationSum = 0;
            for (DoubleWritable value: backupList) {
                deviationSum += Math.pow((value.get()-mean),2);
            }
            standardDeviation.set(Math.sqrt(deviationSum/(N-1)));
            System.out.println("Desvio padrao: " +Math.sqrt(deviationSum/(N-1)));
            context.write(key, standardDeviation);
        }
    }
}
