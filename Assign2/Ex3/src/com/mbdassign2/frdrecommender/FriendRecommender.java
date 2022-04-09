package com.mbdassign2.frdrecommender;

import java.util.Map.Entry;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;


public class FriendRecommender {

    static final int MAX_RECOMMENDATION_COUNT = 10;

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "FriendRecommenderSystem");
        job.setJarByClass(FriendRecommender.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }

    public static class Map extends Mapper<LongWritable, Text, IntWritable, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String friendA, friendB;
            IntWritable friendAKey = new IntWritable();
            Text friendAVal = new Text();
            IntWritable friendBKey = new IntWritable();
            Text friendBVal = new Text();
            StringTokenizer tokenizer = new StringTokenizer(line, "\t");
            if(tokenizer.countTokens() == 2) {
                while(tokenizer.hasMoreTokens()) {
                    IntWritable user = new IntWritable(Integer.parseInt(tokenizer.nextToken()));
                    String[] friends = tokenizer.nextToken().split(",");
                    for (int i = 0; i < friends.length; i++) {
                        friendA = friends[i];
                        friendAVal.set("1," + friendA);
                        context.write(user, friendAVal);
                        friendAKey.set(Integer.parseInt(friendA));
                        friendAVal.set("2," + friendA);
                        for (int j = i+1; j < friends.length; j++) {
                            friendB = friends[j];
                            friendBKey.set(Integer.parseInt(friendB));
                            friendBVal.set("2," + friendB);
                            context.write(friendAKey, friendBVal);
                            context.write(friendBKey, friendAVal);
                        }
                    }
                }
            }
        }
    }

    public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text> {
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] value;
            HashMap<String, Integer> hash = new HashMap<>();
            while(values.iterator().hasNext()) {
                Text val = values.iterator().next();
                value = (val.toString()).split(",");
                if (value[0].equals("1")) {
                    hash.put(value[1], -1);
                } else if (value[0].equals("2")) {
                    if (hash.containsKey(value[1])) {
                        if (hash.get(value[1]) != -1) {
                            hash.put(value[1], hash.get(value[1]) + 1);
                        }
                    } else {
                        hash.put(value[1], 1);
                    }
                }
            }

            ArrayList<Entry<String, Integer>> list = new ArrayList<>();
            hash.entrySet().removeIf(entry -> entry.getValue() == -1);
            list.addAll(hash.entrySet());
            Collections.sort(list, (e1, e2) -> e2.getValue().compareTo(e1.getValue()));
            if (list.size() > MAX_RECOMMENDATION_COUNT) {
                list = new ArrayList<>(list.subList(0, MAX_RECOMMENDATION_COUNT));
            } else {
                list = new ArrayList<>(list);
            }
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < list.size(); i++) {
                sb.append(list.get(i).getKey() + ",");
            }
                context.write(key, new Text(sb.toString()));
            }
        }
}
