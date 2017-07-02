package br.com.mapreduce.minimoquadrado;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

class LeastSquareReducer extends Reducer<DoubleWritable, DoubleWritable, Text, DoubleWritable> {
    private DoubleWritable b = new DoubleWritable(0);
    @Override
    protected void reduce(DoubleWritable x, Iterable<DoubleWritable> ys, Context context) throws IOException, InterruptedException {
        double xMean = context.getConfiguration().getDouble(JobMinimoQuadrado.CONF_NAME_MEAN_X, 0);
        double yMean = context.getConfiguration().getDouble(JobMinimoQuadrado.CONF_NAME_MEAN_Y, 0);
        for (DoubleWritable y : ys) {
            double numerator = x.get() * (y.get() - yMean);
            double denominator = x.get() * (x.get() - xMean);
            b.set( b.get() + (numerator/denominator) );
        }
        context.write(new Text(""), b);
        System.out.println("b = " + b.get());
    }
}
