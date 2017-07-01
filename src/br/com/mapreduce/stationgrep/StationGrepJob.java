package br.com.mapreduce.stationgrep;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Arrays;

import br.com.mapreduce.Constants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.CommandFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class StationGrepJob extends Configured implements Tool{
    public static final String NAME = "StationGrepJob";
    public static final int RESULT_CODE_SUCCESS = 1;
    private static final int RESULT_CODE_FAILED = 0;
    static final String CONF_NAME_STATION = "CONF_NAME_STATION";

    public int run(String[] args) throws Exception {
        if(args.length < 3){
            System.out.println(Constants.GREP_ESTACAO_ARGS);
            //arguments are not enough, input and outputs paths must be passed in the firsts parameters
            throw new CommandFormat.NotEnoughArgumentsException(3, args.length);
        }
        //Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        String stationNumber = args[2];

        Configuration configuration = getConf();
        configuration.setInt(StationGrepJob.CONF_NAME_STATION, Integer.parseInt(stationNumber));

        Job statioGrepJob = new Job(configuration);

        statioGrepJob.setJarByClass(getClass());
        statioGrepJob.setJobName(NAME);
        
        statioGrepJob.setReducerClass(StationReducer.class);
        
        statioGrepJob.setMapOutputKeyClass(LongWritable.class);
        statioGrepJob.setMapOutputValueClass(Text.class);

        statioGrepJob.setOutputValueClass(Text.class);
        statioGrepJob.setOutputValueClass(Text.class);

        //BAGUNÇA DO CASSINI
		File file = new File(args[0]);
		String[] directories = file.list(new FilenameFilter() {
		  @Override
		  public boolean accept(File current, String name) {
		    return new File(current, name).isDirectory();
		  }
		});


		String data_inicio = args[3];
		String data_fim = args[4];
		String cIn = data_inicio.substring(0, Math.min(data_inicio.length(), 4));
		String cFin = data_fim.substring(0, Math.min(data_fim.length(), 4));
		
		Arrays.sort(directories);
		
		int i = 0;
		while (!directories[i].contains(cIn)){
			i++;
		}
		
		while (!directories[i].contains(cFin)){
			MultipleInputs.addInputPath(statioGrepJob,new Path((args[0] + directories[i])),TextInputFormat.class,StationMapper.class);
			i++;		
		}
		MultipleInputs.addInputPath(statioGrepJob,new Path((args[0] + directories[i])),TextInputFormat.class,StationMapper.class);
		
		
		//for (int i =34;i<47;i++){
			//Path inputPath = new Path((args[0] + directories[i]));
		//	MultipleInputs.addInputPath(statioGrepJob,new Path((args[0] + directories[i])),TextInputFormat.class,StationMapper.class);
		//	MultipleInputs.addInputPath(statioGrepJob,new Path((args[0] + "1940")),TextInputFormat.class,StationMapper.class);
		//	MultipleInputs.addInputPath(statioGrepJob,new Path((args[0] + "1941")),TextInputFormat.class,StationMapper.class);
			
			//}
        //CABOU BAGUNCINHA
        
        //FileInputFormat.setInputPaths(statioGrepJob, inputPath);
        FileOutputFormat.setOutputPath(statioGrepJob, outputPath);

        //statioGrepJob.setMapperClass(StationMapper.class);
        



        boolean completed = statioGrepJob.waitForCompletion(true);
        if(completed){
            return RESULT_CODE_SUCCESS;
        }
        return RESULT_CODE_FAILED;
    }
}






