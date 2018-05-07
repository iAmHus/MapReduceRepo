package com.huss.mr;


/**
 *
 * This is the Main class for our first MR job
 *
 *
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CharCount {

    public static void main(String[] args) throws Exception {

        System.out.println("The len of args is "+ args.length);

        if(args.length !=2){
            System.err.println("Invalid Command");
            System.err.println("Usage: CharCount <input path> <output path>");
            System.exit(0);
        }
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(CharCount.class);
        job.setJobName("Char Count");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(CharCountMapper.class);
        job.setReducerClass(CharCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true)?0:1);
    }
}
