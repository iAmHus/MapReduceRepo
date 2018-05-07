package com.huss.mr;

/**
 * The Mapper class for CharCount. It outputs key value pairs of the form (word,1)
 *
 */



import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class CharCountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

    String line = value.toString();

    for (String word : line.split(" ")) {

        if(word.length()>0){
            context.write(new Text(word.substring(0,1)),new IntWritable(1));
        }
    }
}

}
