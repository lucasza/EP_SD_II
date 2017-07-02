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
// import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

public class DesvioPadrao extends Configured implements Tool {
    public static final String NOME = "Desvio Padrão";
    private static final int FALHA = 0;
    private static final int SUCESSO = 1;
    static final String CONF_NAME_MEASUREMENT = "CONF_NAME_MEASUREMENT"; 
    public static final String FAIXA_X = "FAIXA_X";
    private String diretorioTemp1;
    private String diretorioTemp2;

    private double desvioPadrao;

    public double getDesvioPadrao() {
        return desvioPadrao;
    }

    public int run(String[] args) throws Exception {
        if(args.length < 7) {
            System.out.println(Main.DESVIO_PADRAO_ARGS);
            throw new CommandFormat.NotEnoughArgumentsException(7, args.length);
        }
        String entrada = args[1];
        String saida = args[2];
        String estacao = args[3];
        String dataIni = args[4];
        String dataFim = args[5];
        String medida = args[6];
        String faixa = args[7];

        Configuration config = getConf();
        config.set(CONF_NAME_MEASUREMENT, medida);
        config.set(FAIXA_X, faixa);

        if(estacao != null && !estacao.equals("") ) {
            entrada = runStationGrepJob(entrada, estacao, dataIni, dataFim);
        }

        if(dataIni != null &&  !dataIni.equals("") && dataFim != null &&  !dataFim.equals("") ) {
            entrada = runDateGrepJob(entrada, dataIni, dataFim);
        }

        @SuppressWarnings("deprecation")
		Job stdDevJob = new Job(config);
        stdDevJob.setJarByClass(getClass());
        stdDevJob.setJobName(NOME);
        
        saida = saida + System.currentTimeMillis();

        FileInputFormat.setInputPaths(stdDevJob, new Path(entrada));
        FileOutputFormat.setOutputPath(stdDevJob, new Path(saida));

        stdDevJob.setMapperClass(MapDesvioPadrao.class);
        stdDevJob.setReducerClass(ReduceDesvioPadrao.class);

        stdDevJob.setOutputKeyClass(Text.class);
        stdDevJob.setOutputValueClass(DoubleWritable.class);
        boolean completed = stdDevJob.waitForCompletion(true);
        /*
        FileSystem.get(getConf()).delete(new Path(mDateGrepTempDir), true);
        FileSystem.get(getConf()).delete(new Path(mStationGrepTempDir), true);
        */

        if(completed){
            Scanner scanner = Main.getScanner(saida);
            scanner.next();
            this.desvioPadrao = Double.parseDouble(scanner.next());
            return SUCESSO;
        }
        return FALHA;
    }

    private String runDateGrepJob(String inputPath, String dateBegin, String dateEnd) {
        BuscaData procuraDataJob = new BuscaData();

        diretorioTemp1 = "temporario/desvio_padrao/data/data-temp-" + System.currentTimeMillis();
        int runCode;
        try {
            runCode = ToolRunner.run(procuraDataJob, new String[]{inputPath, diretorioTemp1, dateBegin, dateEnd});
            if(runCode == BuscaData.RESULT_CODE_SUCCESS) {
                System.out.println(BuscaData.NAME + " success :)");
                return diretorioTemp1;
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
        diretorioTemp2 = "temporario/desvio_padrao/estacao/estacao-temp-" + System.currentTimeMillis();
        int runCode;
        try {
            runCode = ToolRunner.run(stationGrepJob, new String[]{inputPath, diretorioTemp2, stationNumber,dateBegin, dateEnd});
            if(runCode == BuscaEstacao.RESULT_CODE_SUCCESS) {
                System.out.println(BuscaEstacao.NAME + " success :)");
                return diretorioTemp2;
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
    
    private static class MapDesvioPadrao extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        @Override

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String measurement = context.getConfiguration().get(DesvioPadrao.CONF_NAME_MEASUREMENT);
            String faixa = context.getConfiguration().get(DesvioPadrao.FAIXA_X);
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

    private static class ReduceDesvioPadrao extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable standardDeviation = new DoubleWritable();
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            String measurement = context.getConfiguration().get(DesvioPadrao.CONF_NAME_MEASUREMENT);
            //if (key.equals( new Text(measurement))) {
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
           // }
        }
    }
}