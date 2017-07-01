package br.com.mapreduce;

import br.com.mapreduce.leastsquare.LeastSquareJob;
import br.com.mapreduce.mean.MeanJob;
import br.com.mapreduce.stddeviation.StdDeviationJob;
import org.apache.hadoop.util.ToolRunner;
import java.io.File;

public class Main {
    public static void main(String[] args){
        if(args.length > 0) {
            Utils.inicializaDadosInvalidos();
            String command = args[0];
            if (command.equals(Constants.COMMAND_LEAST_SQUARE)) {
                runLeastSquare(args);
            }
            else if (command.equals(Constants.COMMAND_MEAN)) {
                runMean(args);
            }
            else if (command.equals(Constants.COMMAND_STD_DEVIATION)) {
                runStandardDeviation(args);
            }
            else { //When the first arguments doesn't match with any command option
                printManual();
            }
        }
        else{
            printManual();
        }
    }

    private static void printManual(){
        //TODO - print command options and explanation
        System.out.println("Command options:");
        System.out.println("\t" + Constants.COMMAND_LEAST_SQUARE + " - " + "\n\t" + Constants.COMMAND_ARGUMENTS_LEAST_SQUARE +
                "\n\t" + Constants.COMMAND_EXPLANATION_LEAST_SQUARE);
        System.out.println("\t" +Constants.COMMAND_MEAN + " - " +"\n\t" +Constants.COMMAND_ARGUMENTS_MEAN +
                "\n\t" +Constants.COMMAND_EXPLANATION_MEAN);
        System.out.println("\t" +Constants.COMMAND_STD_DEVIATION +" - " +"\n\t" +Constants.COMMAND_ARGUMENTS_STD_DEVIATION +
                "\n\t" +Constants.COMMAND_EXPLANATION_STD_DEVIATION);
    }

    private static void runStandardDeviation(String args[]) {
        StdDeviationJob stdDeviationJob = new StdDeviationJob();
        try {
            int runCode = ToolRunner.run(stdDeviationJob, args);
            if(runCode == LeastSquareJob.RESULT_CODE_SUCCESS) {
                System.out.println(StdDeviationJob.NAME + " success :)");
                System.out.println(StdDeviationJob.NAME + " success :)");
                System.out.println(StdDeviationJob.NAME + " success :)");
                double stdDeviation = stdDeviationJob.getStandardDeviation();
                System.out.println("standard deviation = " + stdDeviation);
                System.out.println("standard deviation = " + stdDeviation);
                System.out.println("standard deviation = " + stdDeviation);
            }
        } catch (Exception e) {
            System.out.println("Error executing " + LeastSquareJob.NAME);
            e.printStackTrace();
        }
    }

    private static void runLeastSquare(String[] args){
        LeastSquareJob leastSquareJob = new LeastSquareJob();
        try {
            int runCode = ToolRunner.run(leastSquareJob, args);
            if(runCode == LeastSquareJob.RESULT_CODE_SUCCESS) {
                System.out.println(LeastSquareJob.NAME + " success :)");
                System.out.println(LeastSquareJob.NAME + " success :)");
                System.out.println(LeastSquareJob.NAME + " success :)");

                double x = Double.parseDouble(args[args.length - 1]);
                double leastSquare = leastSquareJob.getLeastSquare(x);
                System.out.println("least square = " + leastSquare);
                System.out.println("least square = " + leastSquare);
                System.out.println("least square = " + leastSquare);
            } else {
                System.out.println(LeastSquareJob.NAME + " failed :(");
            }
        } catch (Exception e) {
            System.out.println("Error executing " + LeastSquareJob.NAME);
            e.printStackTrace();
        }
    }

    private static void runMean(String[] args){
        MeanJob meanJob = new MeanJob();
        try {
            int runCode = ToolRunner.run(meanJob, args);
            if(runCode == MeanJob.RESULT_CODE_SUCCESS) {
                System.out.println(MeanJob.NAME + " success :)");
                System.out.println(MeanJob.NAME + " success :)");
                System.out.println(MeanJob.NAME + " success :)");
                double mean = meanJob.getMean();
                System.out.println("mean = " + mean);
                System.out.println("mean = " + mean);
                System.out.println("mean = " + mean);
            } else {
                System.out.println(MeanJob.NAME + " failed :(");
                System.out.println(MeanJob.NAME + " failed :(");
                System.out.println(MeanJob.NAME + " failed :(");
            }
        } catch (Exception e) {
            System.out.println("Error executing " + MeanJob.NAME);
            System.out.println("Error executing " + MeanJob.NAME);
            System.out.println("Error executing " + MeanJob.NAME);
            e.printStackTrace();
        }
    }
}
