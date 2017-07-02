package br.com.mapreduce.minimoquadrado;

import br.com.mapreduce.BuscaData;
import br.com.mapreduce.BuscaEstacao;
import br.com.mapreduce.Main;
import br.com.mapreduce.Media;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.CommandFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Scanner;

public class JobMinimoQuadrado extends Configured implements Tool {

    public static final String NAME = "LeastSquareJob";

    private static final int RESULT_CODE_FAILED = 0;
    public static final int RESULT_CODE_SUCCESS = 1;

    static final String CONF_NAME_MEASUREMENT = "CONF_NAME_MEASUREMENT";
    static final String CONF_NAME_MEAN_X = "CONF_NAME_MEAN_X";
    static final String CONF_NAME_MEAN_Y = "CONF_NAME_MEAN_Y";

    private String mDateGrepTempDir;
    private String mStationGrepTempDir;
    private String mMeanTempDir;

    private double b;

    public int run(String[] args) throws Exception {
        int argsExcepted = 8;
        if(args.length < argsExcepted){
            System.out.println(Main.MINIMO_QUADRADO_ARGS);
            //arguments are not enough, input and outputs paths must be passed in the firsts parameters
            throw new CommandFormat.NotEnoughArgumentsException(argsExcepted, args.length);
        }
        String inputPath = args[1];
        String outputPath = args[2];
        String stationNumber = args[3];
        String dateBegin = args[4];
        String dateEnd = args[5];
        String measurement = args[6];

        //If the user put a work station filter as parameter, we have to run the job to filter this
        if(stationNumber != null && !stationNumber.equals("") ) {
            //if the filter by date was succeed, update the inputPath for the next Job work
            inputPath = runStationGrepJob(inputPath, stationNumber, dateBegin, dateEnd);
        }

        //If the user put a work begin date and end date as parameter, we have to run the job to filter this
        if(dateBegin != null &&  !dateBegin.equals("") && dateEnd != null &&  !dateEnd.equals("") ) {
            //if the filter by date was succeed, update the inputPath for the next Job work
            inputPath = runDateGrepJob(inputPath, dateBegin, dateEnd);
        }

        //Set params of job inside the Configuration
        Configuration configuration = getConf();
        configuration.set(CONF_NAME_MEASUREMENT, measurement);
        long xMean = (long) getMean(inputPath, Main.COLUNAS[2]);
        configuration.setLong(CONF_NAME_MEAN_X, xMean);
        long yMean = (long) getMean(inputPath, measurement);
        configuration.setLong(CONF_NAME_MEAN_Y, yMean);
        if(xMean != 0 || yMean != 0) {
            //return RESULT_CODE_SUCCESS;
        }

        Job leastSquareJob = new Job(configuration);
        leastSquareJob.setJarByClass(getClass());
        leastSquareJob.setJobName(NAME);

       // outputPath = outputPath + System.currentTimeMillis();
        
        FileInputFormat.setInputPaths(leastSquareJob, new Path(inputPath));
        FileOutputFormat.setOutputPath(leastSquareJob, new Path(outputPath));

        leastSquareJob.setMapperClass(LeastSquareMapper.class);
        leastSquareJob.setReducerClass(LeastSquareReducer.class);

        leastSquareJob.setMapOutputKeyClass(DoubleWritable.class);
        leastSquareJob.setMapOutputValueClass(DoubleWritable.class);

        leastSquareJob.setOutputKeyClass(Text.class);
        leastSquareJob.setOutputValueClass(DoubleWritable.class);

        boolean completed = leastSquareJob.waitForCompletion(true);
        /*
        FileSystem.get(getConf()).delete(new Path(mDateGrepTempDir), true);
        FileSystem.get(getConf()).delete(new Path(mStationGrepTempDir), true);
        */

        if(completed){
            Scanner scanner = Main.getScanner(outputPath);
            String line = scanner.nextLine();
            while (scanner.hasNext()){
                line = scanner.nextLine();
            }
            this.b = Double.parseDouble(line.trim());

            return RESULT_CODE_SUCCESS;
        }
        return RESULT_CODE_FAILED;
    }

    public double getLeastSquareMax(double x) {
        double xMean = getConf().getDouble(JobMinimoQuadrado.CONF_NAME_MEAN_X, 0);
        double yMean = getConf().getDouble(JobMinimoQuadrado.CONF_NAME_MEAN_Y, 0);
        double a = yMean - this.b * xMean;
        double minimo = a + this.b * x;
        return minimo;

    }
    
    public double getLeastSquareMin() {
        double xMean = getConf().getDouble(JobMinimoQuadrado.CONF_NAME_MEAN_X, 0);
        double yMean = getConf().getDouble(JobMinimoQuadrado.CONF_NAME_MEAN_Y, 0);
        double a = yMean - this.b * xMean;
        double minimo = a + this.b;
        return minimo;

    }

    private double getMean(String inputPath, String measurement) {
        try {
            Media meanJob = new Media();
            
            mMeanTempDir = "temporario/minimos_quadrados/media/media-temp-" + System.currentTimeMillis();
            int runCode = ToolRunner.run(meanJob, new String[]{"", inputPath, mMeanTempDir, "", "", "", measurement,"Tudo"});
            if(runCode == Media.RESULT_CODE_SUCCESS) {
                double mean = meanJob.getMean();
                System.out.println(Media.NAME + " success :) = " + mean);
                return mean;
            } else {
                System.out.println(Media.NAME + " failed :(");
            }
        } catch (Exception e) {
            System.out.println("Error executing " + Media.NAME);
            e.printStackTrace();
        }
        return 0;
    }

    private String runDateGrepJob(String inputPath, String dateBegin, String dateEnd) {
        BuscaData procuraDataJob = new BuscaData();

        mDateGrepTempDir = "temporario/minimos_quadrados/data/data-temp-" + System.currentTimeMillis();
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

    private String runStationGrepJob(String inputPath, String stationNumber, String dateBegin, String dateEnd) {
        BuscaEstacao stationGrepJob = new BuscaEstacao();
        mStationGrepTempDir = "temporario/minimos_quadrados/estacao/estacao-temp-" + System.currentTimeMillis();
        int runCode;
        try {
            runCode = ToolRunner.run(stationGrepJob, new String[]{inputPath, mStationGrepTempDir, stationNumber, dateBegin, dateEnd});
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
}
