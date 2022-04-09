package test.mbdassign.wordfreq;

import com.mbdassign.wordfreq.CustomWordCount;
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


public class CustomWordCountMRUnit {
    MapDriver<Object, Text, Text, IntWritable> mapDriver;
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void setUp() {
        CustomWordCount.TokenizerMapper mapper = new CustomWordCount.TokenizerMapper();
        CustomWordCount.IntSumReducer reducer = new CustomWordCount.IntSumReducer();
        Configuration conf = new Configuration();
        conf.set("io.serializations","org.apache.hadoop.io.serializer.JavaSerialization,"
                + "org.apache.hadoop.io.serializer.WritableSerialization");
        mapDriver = MapDriver.newMapDriver(mapper);
        mapDriver.setConfiguration(conf);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(1), new Text("The Project Gutenberg Etext:"));
        mapDriver.withOutput(new Text("3"), new IntWritable(1));
        mapDriver.withOutput(new Text("7"), new IntWritable(1));
        mapDriver.withOutput(new Text("9"), new IntWritable(1));
        mapDriver.withOutput(new Text("5"), new IntWritable(1));
        mapDriver.runTest();
        System.out.println("Mapper Unit Test Cases Success");
    }

    @Test
    public void testReducer() throws IOException {
        reduceDriver.withInput(new Text("3"), getTestData(1));
        reduceDriver.withInput(new Text("4"), getTestData(4));
        reduceDriver.withInput(new Text("2"), getTestData(2));
        reduceDriver.withOutput(new Text("3"), new IntWritable(1));
        reduceDriver.withOutput(new Text("4"), new IntWritable(4));
        reduceDriver.withOutput(new Text("2"), new IntWritable(2));
        reduceDriver.runTest();
        System.out.println("Reducer Unit Test Cases Success");
    }

    private  List<IntWritable> getTestData(int count) {
        List<IntWritable> values = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            values.add(new IntWritable(1));
        }
        return values;
    }

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver.withInput(new LongWritable(1), new Text("Hello World, Map Reduce Hello Testing"));
        mapReduceDriver.withOutput(new Text("3"), new IntWritable(1));
        mapReduceDriver.withOutput(new Text("5"), new IntWritable(3));
        mapReduceDriver.withOutput(new Text("6"), new IntWritable(1));
        mapReduceDriver.withOutput(new Text("7"), new IntWritable(1));

        mapReduceDriver.runTest();
        System.out.println("MapReduce Unit Test Cases Success");
    }
}