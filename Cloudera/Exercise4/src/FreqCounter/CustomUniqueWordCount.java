package FreqCounter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.StringTokenizer;

public class CustomUniqueWordCount {

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

    public static class UniqueMapper
            extends Mapper<Text, IntWritable, Text, IntWritable> {
        private Set<String> uniqueWords = new HashSet<>();
        private Text word = new Text();

        public void map(Text key, IntWritable value, Context context
        ) throws IOException, InterruptedException {
            if (uniqueWords.add(key.toString())) {
                word.set(key);
                context.write(word, value);
            }
        }
    }

    public static class WordLengthMapper
            extends Mapper<Text, IntWritable, Text, IntWritable> {
        private Text word = new Text();

        public void map(Text key, IntWritable value, Context context
        ) throws IOException, InterruptedException {
            word.set(String.valueOf(key.toString().length()));
            context.write(word, value);
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }



    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance();

        Configuration splitMap = new Configuration(false);
        ChainMapper.addMapper(job, TokenizerMapper.class, Object.class,
                Text.class, Text.class, IntWritable.class, splitMap);
        Configuration uniqueWords = new Configuration(false);
        ChainMapper.addMapper(job, UniqueMapper.class, Text.class,
                IntWritable.class, Text.class, IntWritable.class,
                uniqueWords);
        Configuration wordLengthMapper = new Configuration(false);
        ChainMapper.addMapper(job, WordLengthMapper.class, Text.class,
                IntWritable.class, Text.class, IntWritable.class, wordLengthMapper);

        job.setJarByClass(CustomUniqueWordCount.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}