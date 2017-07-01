package br.com.mapreduce;

import br.com.mapreduce.leastsquare.JobMinimoQuadrado;
import br.com.mapreduce.mean.MeanJob;
import br.com.mapreduce.stddeviation.JobDesvioPadrao;
import org.apache.hadoop.util.ToolRunner;
import java.io.File;

public class Main {
    public static void main(String[] args){
        if(args.length > 0) {
            Utils.inicializaDadosInvalidos();
            String comando = args[0];
            if (comando.equals(Constants.MINIMO_QUADRADO)) {
                minimoQuadrado(args);
            }
            else if (comando.equals(Constants.COMMAND_MEAN)) {
                media(args);
            }
            else if (comando.equals(Constants.DESVIO_PADRAO)) {
                desvioPadrao(args);
            }
            else { //When the first arguments doesn't match with any command option
                imprimeManual();
            }
        }
        else{
            imprimeManual();
        }
    }

    private static void imprimeManual(){
        System.out.println("Command options:");
        System.out.println("\t" + Constants.MINIMO_QUADRADO + " - " + "\n\t" + Constants.MINIMO_QUADRADO_ARGS +
                "\n\t" + Constants.EXPLICACAO_MINIMO_QUADRADO);
        System.out.println("\t" +Constants.COMMAND_MEAN + " - " +"\n\t" +Constants.COMMAND_ARGUMENTS_MEAN +
                "\n\t" +Constants.COMMAND_EXPLANATION_MEAN);
        System.out.println("\t" +Constants.DESVIO_PADRAO +" - " +"\n\t" +Constants.DESVIO_PADRAO_ARGS +
                "\n\t" +Constants.EXPLICACAO_DESVIO_PADRAO);
    }

    private static void desvioPadrao(String args[]) {
        JobDesvioPadrao jobDesvioPadrao = new JobDesvioPadrao();
        try {
            int tentativa = ToolRunner.run(jobDesvioPadrao, args);
            if(tentativa == JobMinimoQuadrado.RESULT_CODE_SUCCESS) {
                double desvioPadrao = jobDesvioPadrao.getStandardDeviation();
                System.out.println("Desvio padrão = " + desvioPadrao);
            }
        } catch (Exception e) {
            System.out.println("Erro ao executar " + JobMinimoQuadrado.NAME);
            e.printStackTrace();
        }
    }

    private static void minimoQuadrado(String[] args){
        JobMinimoQuadrado jobMinimoQuadrado = new JobMinimoQuadrado();
        try {
            int tentativa = ToolRunner.run(jobMinimoQuadrado, args);
            if(tentativa == JobMinimoQuadrado.RESULT_CODE_SUCCESS) {
                System.out.println(JobMinimoQuadrado.NAME + " completado.");
                double x = Double.parseDouble(args[args.length - 1]);
                double minimoQuadrado = jobMinimoQuadrado.getLeastSquare(x);
                System.out.println("Mínimo Quadrado = " + minimoQuadrado);
            }
        } catch (Exception e) {
            System.out.println("Erro ao executar " + JobMinimoQuadrado.NAME);
            e.printStackTrace();
        }
    }

    private static void media(String[] args){
        MeanJob jobMedia = new MeanJob();
        try {
            int tentativa = ToolRunner.run(jobMedia, args);
            if(tentativa == MeanJob.RESULT_CODE_SUCCESS) {
                System.out.println(MeanJob.NAME + " completado.");
                double media = jobMedia.getMean();
                System.out.println("Média = " + media);
            }
        } catch (Exception e) {
            System.out.println("Erro ao executar " + MeanJob.NAME);
            e.printStackTrace();
        }
    }
}
