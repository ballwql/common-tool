package com.fit.dba.mr.util;



import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
 
import java.io.IOException;
import java.util.List;
 
public class CombineFileTest extends Configured implements Tool {
    private static final Log LOG = LogFactory.getLog(CombineFileTest.class);
    private static final long ONE_MB = 1024 * 1024L;
 
    static class TextFileMapper extends Mapper<LongWritable , Text, Text, Text> {
 
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            Configuration configuration = context.getConfiguration();
            LOG.warn("#######################" + configuration.get(MRJobConfig.MAP_INPUT_FILE));
            Text filenameKey = new Text(configuration.get(MRJobConfig.MAP_INPUT_FILE));
            context.write(filenameKey, value);
        }
    }
 
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CombineFileTest(), args);
        System.exit(exitCode);
    }
 
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration(getConf());
        conf.setLong("mapreduce.input.fileinputformat.split.maxsize", ONE_MB * 32);
        Job job = Job.getInstance(conf);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setJarByClass(CombineFileTest.class);
        job.setInputFormatClass(CombineTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(TextFileMapper.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }
}