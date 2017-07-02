package br.com.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.CommandFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class BuscaData extends Configured implements Tool {
    public static final String NAME = "DateGrepJob";
    public static final int RESULT_CODE_SUCCESS = 1;
    private static final int RESULT_CODE_FAILED = 0;
    static final String CONF_NAME_DATE_BEGIN = "CONF_NAME_DATE_BEGIN";
    static final String CONF_NAME_DATE_END = "CONF_NAME_DATE_END";

    public int run(String[] args) throws Exception {
        if(args.length < 3){
            System.out.println(Main.FETCH_DATA_ARGS);
            //arguments are not enough, input and outputs paths must be passed in the firsts parameters
            throw new CommandFormat.NotEnoughArgumentsException(3, args.length);
        }
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        String dateBegin = args[2];
        String dateEnd = args[3];

        Configuration configuration = getConf();
        configuration.setInt(CONF_NAME_DATE_BEGIN, Integer.parseInt(dateBegin));
        configuration.setInt(CONF_NAME_DATE_END, Integer.parseInt(dateEnd));

        @SuppressWarnings("deprecation")
		Job dateGrepJob = new Job(configuration);
        dateGrepJob.setJarByClass(getClass());
        dateGrepJob.setJobName(NAME);

        FileInputFormat.setInputPaths(dateGrepJob, inputPath);
        FileOutputFormat.setOutputPath(dateGrepJob, outputPath);

        dateGrepJob.setMapperClass(DateMapper.class);
        dateGrepJob.setReducerClass(DateReducer.class);

        dateGrepJob.setMapOutputKeyClass(LongWritable.class);
        dateGrepJob.setMapOutputValueClass(Text.class);

        dateGrepJob.setOutputValueClass(Text.class);
        dateGrepJob.setOutputValueClass(Text.class);

        boolean completed = dateGrepJob.waitForCompletion(true);
        if(completed){
            return RESULT_CODE_SUCCESS;
        }
        return RESULT_CODE_FAILED;
    }
    
    private static class DateReducer extends BuscaReducer {
        protected boolean isInsideGrep(Context context, LongWritable key) {
            int begin = context.getConfiguration().getInt(BuscaData.CONF_NAME_DATE_BEGIN, -1);
            int end = context.getConfiguration().getInt(BuscaData.CONF_NAME_DATE_END, -1);
            return key.compareTo( new LongWritable( begin ) ) >= 0 &&  key.compareTo( new LongWritable( end ) ) <= 0;
        }
    }
    
    private static class DateMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        @Override
        protected void map(LongWritable inputKey, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\\s+");
            if (tokens.length > 2) {
                String token = tokens[0];
                if (token.charAt(0) == 'S') {
                    return;
                }
                token = tokens[2];

                long outputKey = Long.parseLong(token);
                context.write(new LongWritable(outputKey), value);
                //System.out.println("<" + outputKey + ", " + value.toString().substring(0, 100) + ">");
            }
        }
    }
}
