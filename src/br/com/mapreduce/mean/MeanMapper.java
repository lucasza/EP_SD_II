package br.com.mapreduce.mean;

import br.com.mapreduce.Constants;
import br.com.mapreduce.Utils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MeanMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    @Override

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String measurement = context.getConfiguration().get(MeanJob.CONF_NAME_MEASUREMENT);
        int measurementTokenIndex;
        for(measurementTokenIndex = 0; measurementTokenIndex < Constants.COLUNAS.length; measurementTokenIndex++) {
            if (Constants.COLUNAS[measurementTokenIndex].equals(measurement)) {
                break;
            }
        }

        String[] tokens = value.toString().split("\\s+");
        if (tokens.length > 2) {
            String firsToken = tokens[0];
            if (firsToken.charAt(0) == 'S') {
                return;
            }
            
            String anoMes = tokens[2].substring(0, Math.min(tokens[2].length(), 6));
            Text tokenKey = new Text(measurement+" "+anoMes);
            
            double measureLong = Double.parseDouble(tokens[measurementTokenIndex]);
            DoubleWritable tokenValue = new DoubleWritable(measureLong);

            if (Utils.getInvalidData(measurement) != measureLong) {
                context.write(tokenKey, tokenValue);
            }
            System.out.println("<" + measurement + ", " + measureLong + ">");
        }
    }
}
