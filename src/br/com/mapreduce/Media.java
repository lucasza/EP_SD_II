package br.com.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.CommandFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
// import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Scanner;

public class Media extends Configured implements Tool{
    public static final String NAME = "Mean";
    private static final int RESULT_CODE_FAILED = 0;
    public static final int RESULT_CODE_SUCCESS = 1;
    public static final String CONF_NAME_MEASUREMENT = "CONF_NAME_MEASUREMENT";
    public static final String FAIXA_X = "FAIXA_X";
    private String mDateGrepTempDir;
    private String mStationGrepTempDir;

    private double mean;

    public int run(String[] args) throws Exception {
        if(args.length < 8){
            System.out.println(Main.MEDIA_ARGS);
            //arguments are not enough, input and outputs paths must be passed in the firsts parameters
            throw new CommandFormat.NotEnoughArgumentsException(7, args.length);
        }
        String inputPath = args[1];
        String outputPath = args[2];
        String stationNumber = args[3];
        String dateBegin = args[4];
        String dateEnd = args[5];
        String measurement = args[6];
        String faixa = args[7];

        //Set params of job inside the Configuration
        Configuration configuration = getConf();
        configuration.set(CONF_NAME_MEASUREMENT, measurement);
        configuration.set(FAIXA_X, faixa);

        //If the user put a work station filter as parameter, we have to run the job to filter this
        if(stationNumber != null && !stationNumber.equals("") ) {
            //if the filter by date was succeed, update the inputPath for the next Job work
            inputPath = runStationGrepJob(inputPath, stationNumber,dateBegin,dateEnd);
        }

        //If the user put a work begin date and end date as parameter, we have to run the job to filter this
        if(dateBegin != null &&  !dateBegin.equals("") && dateEnd != null &&  !dateEnd.equals("") ) {
            //if the filter by date was succeed, update the inputPath for the next Job work
            inputPath = runDateGrepJob(inputPath, dateBegin, dateEnd);
        }

        @SuppressWarnings("deprecation")
		Job meanJob = new Job(configuration);
        meanJob.setJarByClass(getClass());
        meanJob.setJobName(NAME);

        outputPath = outputPath + System.currentTimeMillis();
        
        FileInputFormat.setInputPaths(meanJob, new Path(inputPath));
        FileOutputFormat.setOutputPath(meanJob, new Path(outputPath));

        meanJob.setMapperClass(MeanMapper.class);
        meanJob.setReducerClass(MeanReducer.class);

        meanJob.setOutputKeyClass(Text.class);
        meanJob.setOutputValueClass(DoubleWritable.class);
        boolean completed = meanJob.waitForCompletion(true);

        /*
        FileSystem.get(getConf()).delete(new Path(mDateGrepTempDir), true);
        FileSystem.get(getConf()).delete(new Path(mStationGrepTempDir), true);
        */

        if(completed){
            Scanner scanner = Main.getScanner(outputPath);
            scanner.next();
            this.mean = Double.parseDouble(scanner.next());
            return RESULT_CODE_SUCCESS;
        }
        return RESULT_CODE_FAILED;
    }

    public double getMean() {
        return this.mean;
    }

    private String runDateGrepJob(String inputPath, String dateBegin, String dateEnd) {
        BuscaData procuraDataJob = new BuscaData();

        mDateGrepTempDir = "temporario/media/data/data-temp-" + System.currentTimeMillis();
        int runCode;
        try {
            runCode = ToolRunner.run(procuraDataJob, new String[]{inputPath, mDateGrepTempDir, dateBegin, dateEnd});
            if(runCode == BuscaData.RESULT_CODE_SUCCESS) {
                System.out.println(BuscaData.NAME + " success :)");
                return mDateGrepTempDir;
            } else {
                System.out.println(BuscaData.NAME + " failed :(");
                return inputPath;
            }
        } catch (Exception e) {
            System.out.println("Error executing " + BuscaData.NAME);
            e.printStackTrace();
            return inputPath;
        }
    }

    private String runStationGrepJob(String inputPath, String stationNumber,String dateBegin,String dateEnd) {
        BuscaEstacao stationGrepJob = new BuscaEstacao();
        mStationGrepTempDir = "temporario/media/estacao/estacao-temp-" + System.currentTimeMillis();
        int runCode;
        try {
            runCode = ToolRunner.run(stationGrepJob, new String[]{inputPath, mStationGrepTempDir, stationNumber,dateBegin,dateEnd});
            if(runCode == BuscaEstacao.RESULT_CODE_SUCCESS) {
                System.out.println(BuscaEstacao.NAME + " success :)");
                return mStationGrepTempDir;
            } else {
                System.out.println(BuscaEstacao.NAME + " failed :(");
                return inputPath;
            }
        } catch (Exception e) {
            System.out.println("Error executing " + BuscaEstacao.NAME);
            e.printStackTrace();
            System.exit(-1);
            return inputPath;
        }
    }
    
    private static class MeanMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        @Override

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String measurement = context.getConfiguration().get(Media.CONF_NAME_MEASUREMENT);
            String faixa = context.getConfiguration().get(Media.FAIXA_X);
            int measurementTokenIndex;
            for(measurementTokenIndex = 0; measurementTokenIndex < Main.COLUNAS.length; measurementTokenIndex++) {
                if (Main.COLUNAS[measurementTokenIndex].equals(measurement)) {
                    break;
                }
            }

            String[] tokens = value.toString().split("\\s+");
            if (tokens.length > 2) {
                String firsToken = tokens[0];
                if (firsToken.charAt(0) == 'S') {
                    return;
                }
               
                
                Text tokenKey;
                if (faixa.equals("Mensal")){
                	String anoMes = tokens[2].substring(0, Math.min(tokens[2].length(), 6));
                	tokenKey = new Text(measurement+"\t"+anoMes);
                }
                else if (faixa.equals("Anual")){
                	String anoMes = tokens[2].substring(0, Math.min(tokens[2].length(), 4));
                	tokenKey = new Text(measurement+"\t"+anoMes);
                }
                else{
                	tokenKey = new Text(measurement);
                }
                
                
                
                double measureLong = Double.parseDouble(tokens[measurementTokenIndex]);
                DoubleWritable tokenValue = new DoubleWritable(measureLong);

                if (Main.getDadosInvalidos(measurement) != measureLong) {
                    context.write(tokenKey, tokenValue);
                }
                System.out.println("<" + measurement + ", " + measureLong + ">");
            }
        }
    }
    
    private static class MeanReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable finalMean = new DoubleWritable();
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            //String measurement = context.getConfiguration().get(MeanJob.CONF_NAME_MEASUREMENT);
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
}