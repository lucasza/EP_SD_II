package br.com.mapreduce.dategrep;

import br.com.mapreduce.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.CommandFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class DateGrepJob extends Configured implements Tool {
    public static final String NAME = "DateGrepJob";
    public static final int RESULT_CODE_SUCCESS = 1;
    private static final int RESULT_CODE_FAILED = 0;
    static final String CONF_NAME_DATE_BEGIN = "CONF_NAME_DATE_BEGIN";
    static final String CONF_NAME_DATE_END = "CONF_NAME_DATE_END";

    public int run(String[] args) throws Exception {
        if(args.length < 3){
            System.out.println(Constants.GREP_DATA_ARGS);
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
}
