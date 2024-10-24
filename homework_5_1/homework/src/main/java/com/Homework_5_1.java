package com;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Homework_5_1 {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String inputPath = args[0];
        String tempOutputPath = args[1];
        String finalOutputPath = args[2];
        // String stopWordsPath = args[3];
        // String task = args[4];

        // if ("task1".equalsIgnoreCase(task)) {
        Job job1 = Job.getInstance(conf, "Stock Count");
        job1.setJarByClass(Homework_5_1.class);
        job1.setMapperClass(StockCountMapper.class);
        job1.setCombinerClass(StockCountReducer.class); // Combiner added
        job1.setReducerClass(StockCountReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job1, new Path(inputPath));
        FileOutputFormat.setOutputPath(job1, new Path(tempOutputPath));
        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        Job job2 = Job.getInstance(conf, "Sort Stocks by Count");
        job2.setJarByClass(Homework_5_1.class);
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