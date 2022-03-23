package com.mbdassign.wordfreq;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.*;

public class UniqueWordsTwoJobs {

    public static class WordCountMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        private Set<String> uniqueWords = new HashSet<>();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line, " \t");
            while (tokenizer.hasMoreTokens()) {
                String word = tokenizer.nextToken();
                if (uniqueWords.add(word)) {
                    this.word.set(String.valueOf(word.length()));
                    context.write(this.word, one);
                }
            }
        }
    }

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();


        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString().toLowerCase(Locale.ROOT), " \'\"\t\n\r\f//~,.:;?![](){}<>-_*&^%$#@");
            while (itr.hasMoreTokens()) {
                word.set(String.valueOf(itr.nextToken()));
                context.write(word, one);
            }
        }
    }


    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class WordCountReducer
            extends Reducer<Text,IntWritable,Text, Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(String.valueOf(sum));
            context.write(new Text(String.valueOf(key)), result);
        }
    }



    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        String[] inputArgs = new GenericOptionsParser(new Configuration(), args).getRemainingArgs();

        if(inputArgs.length != 2) {
            System.err.println("Usage: WordCount <in> <out>");
            System.exit(2);
        }

        Configuration conf = new Configuration();

        long start = new Date().getTime();
        Job jobTokenizer = Job.getInstance(conf, "tokenizer");

        jobTokenizer.setJarByClass(CustomWordCount.class);
        jobTokenizer.setMapperClass(TokenizerMapper.class);
        jobTokenizer.setReducerClass(IntSumReducer.class);
        jobTokenizer.setOutputKeyClass(Text.class);
        jobTokenizer.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(jobTokenizer, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobTokenizer, new Path(args[1] + "/jobTokenizerOutput"));
        boolean status1 = jobTokenizer.waitForCompletion(true);
        long end = new Date().getTime();
        if(status1) {
            System.out.println("Job 1 " + (end - start) + " milliseconds");
        }

        Configuration conf1 = new Configuration();
        start = new Date().getTime();
        Job JobWordCount = Job.getInstance(conf1, "wordcount");
        JobWordCount.setJarByClass(CustomWordCount.class);
        JobWordCount.setMapperClass(WordCountMapper.class);
        JobWordCount.setReducerClass(WordCountReducer.class);
        JobWordCount.setOutputKeyClass(Text.class);
        JobWordCount.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(JobWordCount, new Path(args[1] + "/jobTokenizerOutput"));
        FileOutputFormat.setOutputPath(JobWordCount, new Path(args[1] + "/uniqueWords"));
        boolean status2 = JobWordCount.waitForCompletion(true);
        end = new Date().getTime();
        if(status2) {
            System.out.println("Job 2 " + (end - start) + " milliseconds");
        }
    }
}
