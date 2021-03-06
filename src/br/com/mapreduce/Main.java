package br.com.mapreduce;

import org.apache.commons.httpclient.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import javax.swing.JOptionPane;

public class Main {
	
	static final String MINIMO_QUADRADO = "minimo";
    static final String MEDIA = "media";
    static final String DESVIO_PADRAO = "desvio";
    public static final String MINIMO_QUADRADO_ARGS = "minimo <input path> <output path> <work station number> <start date (yyyyMMdd)> <end date (yyyyMMdd)> <measure> <x>";
    public static final String FETCH_ESTACAO_ARGS = "station <input path> <output path> <work station number>";
    public static final String FETCH_DATA_ARGS = "date <input path> <output path> <start date (yyyyMMdd)> <end date (yyyyMMdd)>";
    public static final String MEDIA_ARGS = "media <input path> <output path> <work station number> <start date (yyyyMMdd)> <end date (yyyyMMdd)> <measure>";
    public static final String DESVIO_PADRAO_ARGS = "desvio <input path> <output path> <work station number> <start date (yyyyMMdd)> <end date (yyyyMMdd)> <measure>";

    public static final String[] COLUNAS = {
        "STN---",
        "WBAN",
        "YEARMODA",
        "TEMP",
        "DEWP",
        "SLP",
        "STP",
        "VISIB",
        "WDSP",
        "MXSPD",
        "GUST",
        "MAX",
        "MIN",
        "PRCP",
        "SNDP",
        "FRSHTTYEARMODA",
        "TEMP",
        "DEWP",
        "SLP",
        "STP",
        "VISIB",
        "WDSP",
        "MXSPD",
        "GUST",
        "MAX",
        "MIN",
        "PRCP",
        "SNDP",
        "FRSHTT"
    };
	
    private static void desvioPadrao(String args[]) {
        DesvioPadrao jobDesvioPadrao = new DesvioPadrao();
        try {
            int tentativa = ToolRunner.run(jobDesvioPadrao, args);
            if(tentativa == MinimosQuadrados.RESULT_CODE_SUCCESS) {
                double desvioPadrao = jobDesvioPadrao.getDesvioPadrao();
                System.out.println("Desvio padrão = " + desvioPadrao);
            }
        } catch (Exception e) {
            System.out.println("Erro ao executar " + DesvioPadrao.NOME);
            e.printStackTrace();
        }
    }

    private static void minimoQuadrado(String[] args){
        MinimosQuadrados jobMinimoQuadrado = new MinimosQuadrados();
        try {
            int tentativa = ToolRunner.run(jobMinimoQuadrado, args);
            if(tentativa == MinimosQuadrados.RESULT_CODE_SUCCESS) {
                System.out.println(MinimosQuadrados.NAME + " completado.");
                String faixa = args[7];
                String anoMesInicial;
                String anoMesFinal;
                if (faixa.equals("Mensal")){
                	anoMesInicial = args[4].substring(0, Math.min(args[4].length(), 6));
                	anoMesFinal = args[5].substring(0, Math.min(args[5].length(), 6));                	
                }
                else if (faixa.equals("Anual")){
                	anoMesInicial = args[4].substring(0, Math.min(args[4].length(), 4));
                	anoMesFinal = args[5].substring(0, Math.min(args[5].length(), 4));  
                }
                else{
                	anoMesInicial = "1";
                	anoMesFinal = "1";
                }
                
                int dateBegin = Integer.parseInt(anoMesInicial);
                int dateEnd = Integer.parseInt(anoMesFinal);
                
                int diferenca = dateEnd - dateBegin;
                String outputPath = args[2];
                
                String tempo = String.valueOf(System.currentTimeMillis());
                java.nio.file.Path currentRelativePath = Paths.get("");
                String s = currentRelativePath.toAbsolutePath().toString();
                //new File(s+ ).mkdirs();
                
                String caminho = s + Path.SEPARATOR+ "part-r-00000";
                try{
                	File file = new File(caminho);
                    file.delete();
                } catch (Exception e){}

                // System.out.println("Mínimo Quadrado = " + minimoQuadradoMax);
     		    PrintWriter writer = new PrintWriter(caminho, "UTF-8");
                for (int i =-1;i<diferenca;i++){
                	double minimoQuadradoMax = jobMinimoQuadrado.getLeastSquareMax(i);
                	writer.println("Minimos\t"+ minimoQuadradoMax );
                }
                writer.close();
                Runtime run = Runtime.getRuntime();
                String pasta = outputPath + "minimos/";


         		try{
	                run.exec("hadoop fs -mkdir "+ pasta);
         		} catch (IOException e) {
         		   // do something
         		}
         		
                String whatever = "hadoop fs -put "+ caminho + " " + pasta;

                run.exec(whatever);

              //  double minimoQuadradoMax = jobMinimoQuadrado.getLeastSquareMax(dateEnd-dateBegin);
              //  double minimoQuadradoMin = jobMinimoQuadrado.getLeastSquareMin();
               // System.out.println("Mínimo Quadrado = " + minimoQuadradoMax);
         		
         		Configuration conf = new Configuration();
         	    //localhost HDFS system
//         	    conf.set("fs.defaultFS", "hdfs://localhost/");
//         	    conf.set("mapred.job.tracker", "localhost:54310");
//         	    
//         	    FileSystem fs = FileSystem.get(conf);
//         	    Path p = new Path(outputPath);
//         	    Path q = new Path(caminho);
//         	    
         	    //fs.copyFromLocalFile(q, p);
        		
               // filer("/home/lucasza/Área de Trabalho/56/",minimoQuadradoMin,minimoQuadradoMax);
            }
        } catch (Exception e) {
            System.out.println("Erro ao executar " + MinimosQuadrados.NAME);
            e.printStackTrace();
        }
    }
    


    private static void media(String[] args){
        Media jobMedia = new Media();
        try {
            int tentativa = ToolRunner.run(jobMedia, args);
            if(tentativa == Media.RESULT_CODE_SUCCESS) {
                System.out.println(Media.NAME + " completado.");
                double media = jobMedia.getMean();
                System.out.println("Média = " + media);
            }
        } catch (Exception e) {
            System.out.println("Erro ao executar " + Media.NAME);
            e.printStackTrace();
        }
    }

    public static void main(String[] args){
        if(args.length > 0) {
            inicializaDadosInvalidos();
            String comando = args[0];
            if (comando.equals(MINIMO_QUADRADO)) {
                minimoQuadrado(args);
            }
            else if (comando.equals(DESVIO_PADRAO)) {
                desvioPadrao(args);
            }
            else if (comando.equals(MEDIA)) {
                media(args);
            }
        }
    }
    
    private static Map<String, Double> dadosInvalidos;

    public static void inicializaDadosInvalidos() {
        dadosInvalidos = new HashMap<String, Double>();
        dadosInvalidos.put("TEMP", 9999.9);
        dadosInvalidos.put("DEWP", 9999.9);
        dadosInvalidos.put("SLP", 9999.9);
        dadosInvalidos.put("STP", 9999.9);
        dadosInvalidos.put("VISIB", 999.9);
        dadosInvalidos.put("WDSP", 999.9);
        dadosInvalidos.put("GUST", 999.9);
        dadosInvalidos.put("MAX", 9999.9);
        dadosInvalidos.put("MIN", 9999.9);
        dadosInvalidos.put("PRCP", 99.99);
        dadosInvalidos.put("SNDP", 999.9);
    }

    public static Scanner getScanner(String caminhoSaida) throws IOException {
        Path part = new Path(caminhoSaida + Path.SEPARATOR + "part-r-00000");
        FileSystem fs = FileSystem.get(new Configuration());
        return new Scanner(new InputStreamReader(fs.open(part)));
    }

    public static double getDadosInvalidos(String abreviação) {
        dadosInvalidos = new HashMap<String, Double>();
        dadosInvalidos.put("TEMP", 9999.9);
        dadosInvalidos.put("DEWP", 9999.9);
        dadosInvalidos.put("SLP", 9999.9);
        dadosInvalidos.put("STP", 9999.9);
        dadosInvalidos.put("VISIB", 999.9);
        dadosInvalidos.put("WDSP", 999.9);
        dadosInvalidos.put("GUST", 999.9);
        dadosInvalidos.put("MAX", 9999.9);
        dadosInvalidos.put("MIN", 9999.9);
        dadosInvalidos.put("PRCP", 99.99);
        dadosInvalidos.put("SNDP", 999.9);
        return dadosInvalidos.get(abreviação);
    }
}

