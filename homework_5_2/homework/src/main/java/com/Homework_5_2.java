package com;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Homework_5_2 {

    public static void main(String[] args) throws Exception {
        String inputPath = args[0];
        String stopWordsPath = args[1];
        String tempOutputPath = args[2];
        String finalOutputPath = args[3];

        Configuration conf = new Configuration();

        conf.set("stopwords.path", stopWordsPath);
        Job job1 = Job.getInstance(conf, "Word Count");
        job1.setJarByClass(Homework_5_2.class);
        job1.setMapperClass(WordCountMapper.class);
        job1.setCombinerClass(WordCountReducer.class);
        job1.setReducerClass(WordCountReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job1, new Path(inputPath));
        FileOutputFormat.setOutputPath(job1, new Path(tempOutputPath));
        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        Job job2 = Job.getInstance(conf, "Sort Words by Count");
        job2.setJarByClass(Homework_5_2.class);
        job2.setMapperClass(SortMapper.class);
        job2.setReducerClass(SortReducer.class);
        job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);
        job2.setOutputKeyClass(LongWritable.class);
        job2.setOutputValueClass(Text.class);
        job2.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job2, new Path(tempOutputPath));
        FileOutputFormat.setOutputPath(job2, new Path(finalOutputPath));
        if (!job2.waitForCompletion(true)) {
            System.exit(1);
        }
    }
}