package test.mbdassign.wordfreq;

import com.mbdassign.wordfreq.CustomUniqueWordCount;
import com.mbdassign.wordfreq.UniqueWordsTwoJobs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UniqueWordsTwoJobsMRUnit {
    MapDriver<Object, Text, Text, IntWritable> mapDriverTokenizerMapper;
    MapDriver<Object, Text, Text, IntWritable> mapDriverWordCountMapper;
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriverIntSum;
    ReduceDriver<Text, IntWritable, Text, Text> reduceDriverWordCount;
    MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable> jobOneDriver;
    MapReduceDriver<Object, Text, Text, IntWritable, Text, Text> jobTwoDriver;

    @Before
    public void setUp() {
        UniqueWordsTwoJobs.TokenizerMapper tokenizerMapper = new UniqueWordsTwoJobs.TokenizerMapper();
        UniqueWordsTwoJobs.WordCountMapper wordCountMapper = new UniqueWordsTwoJobs.WordCountMapper();
        UniqueWordsTwoJobs.IntSumReducer intSumReducer = new UniqueWordsTwoJobs.IntSumReducer();
        UniqueWordsTwoJobs.WordCountReducer wordCountReducer = new UniqueWordsTwoJobs.WordCountReducer();

        jobOneDriver = MapReduceDriver.newMapReduceDriver(tokenizerMapper, intSumReducer);
        jobTwoDriver = MapReduceDriver.newMapReduceDriver(wordCountMapper, wordCountReducer);


        Configuration conf = new Configuration();
        conf.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization,"
                + "org.apache.hadoop.io.serializer.WritableSerialization");
        mapDriverTokenizerMapper = MapDriver.newMapDriver(tokenizerMapper);
        mapDriverTokenizerMapper.setConfiguration(conf);
        mapDriverWordCountMapper = MapDriver.newMapDriver(wordCountMapper);
        mapDriverWordCountMapper.setConfiguration(conf);
        reduceDriverIntSum = ReduceDriver.newReduceDriver(intSumReducer);
        reduceDriverWordCount = ReduceDriver.newReduceDriver(wordCountReducer);
    }

    @Test
    public void testTokenizerMapper() throws IOException {
        mapDriverTokenizerMapper.withInput(new LongWritable(1), new Text("The quick brown fox jumps"));
        mapDriverTokenizerMapper.withOutput(new Text("the"), new IntWritable(1));
        mapDriverTokenizerMapper.withOutput(new Text("quick"), new IntWritable(1));
        mapDriverTokenizerMapper.withOutput(new Text("brown"), new IntWritable(1));
        mapDriverTokenizerMapper.withOutput(new Text("fox"), new IntWritable(1));
        mapDriverTokenizerMapper.withOutput(new Text("jumps"), new IntWritable(1));
        mapDriverTokenizerMapper.runTest();
        System.out.println("Tokenize Mapper Unit Test Cases Success");
    }

    @Test
    public void testWordCountMapper() throws IOException {
        mapDriverWordCountMapper.withInput(new LongWritable(1), new Text("the quick brown fox jumps"));
        mapDriverWordCountMapper.withOutput(new Text("3"), new IntWritable(1));
        mapDriverWordCountMapper.withOutput(new Text("5"), new IntWritable(1));
        mapDriverWordCountMapper.withOutput(new Text("5"), new IntWritable(1));
        mapDriverWordCountMapper.withOutput(new Text("3"), new IntWritable(1));
        mapDriverWordCountMapper.withOutput(new Text("5"), new IntWritable(1));
        mapDriverWordCountMapper.runTest();
        System.out.println("Word Count Mapper Unit Test Cases Success");
    }

    private List<IntWritable> getTestData(int count) {
        List<IntWritable> values = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            values.add(new IntWritable(1));
        }
        return values;
    }

    @Test
    public void testReducerIntSum() throws IOException {
        reduceDriverIntSum.withInput(new Text("the"), getTestData(2));
        reduceDriverIntSum.withInput(new Text("quick"), getTestData(3));
        reduceDriverIntSum.withOutput(new Text("the"), new IntWritable(0));
        reduceDriverIntSum.withOutput(new Text("quick"), new IntWritable(0));
        reduceDriverIntSum.runTest();
        System.out.println("Int Sum Reducer Unit Test Cases Success");
    }

    @Test
    public void testReducerWordCount() throws IOException {
        reduceDriverWordCount.withInput(new Text("the"), getTestData(2));
        reduceDriverWordCount.withInput(new Text("quick"), getTestData(3));
        reduceDriverWordCount.withOutput(new Text("the"), new Text("2"));
        reduceDriverWordCount.withOutput(new Text("quick"), new Text("3"));
        reduceDriverWordCount.runTest();
        System.out.println("Word Count Reducer Unit Test Cases Success");
    }

    @Test
    public void testJobOne() throws IOException {
        jobOneDriver.withInput(new LongWritable(1), new Text("The quick brown fox jumps"));
        jobOneDriver.withOutput(new Text("brown"), new IntWritable(0));
        jobOneDriver.withOutput(new Text("fox"), new IntWritable(0));
        jobOneDriver.withOutput(new Text("jumps"), new IntWritable(0));
        jobOneDriver.withOutput(new Text("quick"), new IntWritable(0));
        jobOneDriver.withOutput(new Text("the"), new IntWritable(0));

        jobOneDriver.runTest();
        System.out.println("MapReduce Job 1 Unit Test Cases Success");
    }

    @Test
    public void testJobTwo() throws IOException {
        jobTwoDriver.withInput(new LongWritable(1), new Text("the\t0"));
        jobTwoDriver.withInput(new LongWritable(1), new Text("brown\t0"));
        jobTwoDriver.withInput(new LongWritable(1), new Text("quick\t0"));
        jobTwoDriver.withInput(new LongWritable(1), new Text("jumps\t0"));

        jobTwoDriver.withOutput(new Text("1"), new Text("1"));
        jobTwoDriver.withOutput(new Text("3"), new Text("1"));
        jobTwoDriver.withOutput(new Text("5"), new Text("3"));

        jobTwoDriver.runTest();
        System.out.println("MapReduce Job 2 Unit Test Cases Success");
    }

}
