package br.com.mapreduce.stddeviation;

import br.com.mapreduce.Constants;
import br.com.mapreduce.Utils;
import br.com.mapreduce.dategrep.DateGrepJob;
import br.com.mapreduce.stationgrep.StationGrepJob;
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

public class JobDesvioPadrao extends Configured implements Tool {
    public static final String NAME = "Standard Deviation";
    private static final int RESULT_CODE_FAILED = 0;
    private static final int RESULT_CODE_SUCCESS = 1;
    static final String CONF_NAME_MEASUREMENT = "CONF_NAME_MEASUREMENT";
    private String mDateGrepTempDir;
    private String mStationGrepTempDir;

    private double standardDeviation;

    public double getStandardDeviation() {
        return standardDeviation;
    }

    public int run(String[] args) throws Exception {
        if(args.length < 7) {
            System.out.println(Constants.DESVIO_PADRAO_ARGS);
            //arguments are not enough, input and outputs paths must be passed in the firsts parameters
            throw new CommandFormat.NotEnoughArgumentsException(7, args.length);
        }
        String inputPath = args[1];
        String outputPath = args[2];
        String stationNumber = args[3];
        String dateBegin = args[4];
        String dateEnd = args[5];
        String measurement = args[6];

        //Set params of job inside the Configuration
        Configuration configuration = getConf();
        configuration.set(CONF_NAME_MEASUREMENT, measurement);

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

        Job stdDevJob = new Job(configuration);
        stdDevJob.setJarByClass(getClass());
        stdDevJob.setJobName(NAME);

        FileInputFormat.setInputPaths(stdDevJob, new Path(inputPath));
        FileOutputFormat.setOutputPath(stdDevJob, new Path(outputPath));

        stdDevJob.setMapperClass(StdDeviationMapper.class);
        stdDevJob.setReducerClass(StdDeviationReducer.class);

        stdDevJob.setOutputKeyClass(Text.class);
        stdDevJob.setOutputValueClass(DoubleWritable.class);
        boolean completed = stdDevJob.waitForCompletion(true);
        /*
        FileSystem.get(getConf()).delete(new Path(mDateGrepTempDir), true);
        FileSystem.get(getConf()).delete(new Path(mStationGrepTempDir), true);
        */

        if(completed){
            Scanner scanner = Utils.getScanner(outputPath);
            scanner.next();
            this.standardDeviation = Double.parseDouble(scanner.next());
            return RESULT_CODE_SUCCESS;
        }
        return RESULT_CODE_FAILED;
    }

    private String runDateGrepJob(String inputPath, String dateBegin, String dateEnd) {
        DateGrepJob dateGrepJob = new DateGrepJob();

        mDateGrepTempDir = "temporario/desvio_padrao/data/data-temp-" + System.currentTimeMillis();
        int runCode;
        try {
            runCode = ToolRunner.run(dateGrepJob, new String[]{inputPath, mDateGrepTempDir, dateBegin, dateEnd});
            if(runCode == DateGrepJob.RESULT_CODE_SUCCESS) {
                System.out.println(DateGrepJob.NAME + " success :)");
                return mDateGrepTempDir;
            } else {
                System.out.println(DateGrepJob.NAME + " failed :(");
                return inputPath;
            }
        } catch (Exception e) {
            System.out.println("Error executing " + DateGrepJob.NAME);
            e.printStackTrace();
            return inputPath;
        }
    }

    private String runStationGrepJob(String inputPath, String stationNumber,String dateBegin,String dateEnd) {
        StationGrepJob stationGrepJob = new StationGrepJob();
        mStationGrepTempDir = "temporario/desvio_padrao/estacao/estacao-temp-" + System.currentTimeMillis();
        int runCode;
        try {
            runCode = ToolRunner.run(stationGrepJob, new String[]{inputPath, mStationGrepTempDir, stationNumber,dateBegin, dateEnd});
            if(runCode == StationGrepJob.RESULT_CODE_SUCCESS) {
                System.out.println(StationGrepJob.NAME + " success :)");
                return mStationGrepTempDir;
            } else {
                System.out.println(StationGrepJob.NAME + " failed :(");
                return inputPath;
            }
        } catch (Exception e) {
            System.out.println("Error executing " + StationGrepJob.NAME);
            e.printStackTrace();
            System.exit(-1);
            return inputPath;
        }
    }
}