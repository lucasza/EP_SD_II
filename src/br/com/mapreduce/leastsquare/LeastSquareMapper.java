package br.com.mapreduce.leastsquare;

import br.com.mapreduce.Constants;
import br.com.mapreduce.Utils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class LeastSquareMapper extends Mapper<LongWritable, Text, DoubleWritable, DoubleWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String measurement = context.getConfiguration().get(LeastSquareJob.CONF_NAME_MEASUREMENT);
        int measurementTokenIndex;
        for(measurementTokenIndex = 0; measurementTokenIndex < Constants.FIELDS.length; measurementTokenIndex++) {
            if (Constants.FIELDS[measurementTokenIndex].equals(measurement)) {
                break;
            }
        }

        String[] tokens = value.toString().split("\\s+");
        if (tokens.length > 2) {
            String firsToken = tokens[0];
            if (firsToken.charAt(0) == 'S') {
                return;
            }

            double dataLong = Double.parseDouble(tokens[2]);
            DoubleWritable date = new DoubleWritable(dataLong);

            double measureLong = Double.parseDouble(tokens[measurementTokenIndex]);
            DoubleWritable measure = new DoubleWritable(measureLong);

            if (Utils.getInvalidData(measurement) != measureLong) {
                context.write(date, measure);
            }
            System.out.println("<" + date + ", " + measure + ">");
        }
    }
}
