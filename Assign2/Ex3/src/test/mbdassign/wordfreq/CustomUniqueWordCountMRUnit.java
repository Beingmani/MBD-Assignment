package test.mbdassign.wordfreq;

import com.mbdassign.wordfreq.CustomUniqueWordCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CustomUniqueWordCountMRUnit {
    MapDriver<Object, Text, Text, IntWritable> mapDriverTokenizer;
    MapDriver<Text, IntWritable, Text, IntWritable> mapDriverUniqueMapper;
    MapDriver<Text, IntWritable, Text, IntWritable> mapDriverWordLength;
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriverIntSum;

    @Before
    public void setUp() {
        CustomUniqueWordCount.TokenizerMapper tokenizerMapper = new CustomUniqueWordCount.TokenizerMapper();
        CustomUniqueWordCount.IntSumReducer intSumReducer = new CustomUniqueWordCount.IntSumReducer();
        CustomUniqueWordCount.UniqueMapper uniqueMapper = new CustomUniqueWordCount.UniqueMapper();
        CustomUniqueWordCount.WordLengthMapper wordLengthMapper = new CustomUniqueWordCount.WordLengthMapper();
        Configuration conf = new Configuration();
        conf.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization,"
                + "org.apache.hadoop.io.serializer.WritableSerialization");
        mapDriverTokenizer = MapDriver.newMapDriver(tokenizerMapper);
        mapDriverTokenizer.setConfiguration(conf);
        mapDriverUniqueMapper = MapDriver.newMapDriver(uniqueMapper);
        mapDriverUniqueMapper.setConfiguration(conf);
        mapDriverWordLength = MapDriver.newMapDriver(wordLengthMapper);
        mapDriverWordLength.setConfiguration(conf);
        reduceDriverIntSum = ReduceDriver.newReduceDriver(intSumReducer);
    }


    @Test
    public void testTokenizerMapper() throws IOException {
        mapDriverTokenizer.withInput(new LongWritable(1), new Text("The quick brown fox jumps"));
        mapDriverTokenizer.withOutput(new Text("the"), new IntWritable(1));
        mapDriverTokenizer.withOutput(new Text("quick"), new IntWritable(1));
        mapDriverTokenizer.withOutput(new Text("brown"), new IntWritable(1));
        mapDriverTokenizer.withOutput(new Text("fox"), new IntWritable(1));
        mapDriverTokenizer.withOutput(new Text("jumps"), new IntWritable(1));
        mapDriverTokenizer.runTest();
        System.out.println("Tokenize Mapper Unit Test Cases Success");
    }

    @Test
    public void testUniqueMapper() throws IOException {
        mapDriverUniqueMapper.withInput(new Text("the"), new IntWritable(1));
        mapDriverUniqueMapper.withInput(new Text("quick"), new IntWritable(1));
        mapDriverUniqueMapper.withInput(new Text("brown"), new IntWritable(1));
        mapDriverUniqueMapper.withInput(new Text("fox"), new IntWritable(1));
        mapDriverUniqueMapper.withInput(new Text("jumps"), new IntWritable(1));
        // Duplicates to check if the mapper is removing duplicates
        mapDriverUniqueMapper.withInput(new Text("the"), new IntWritable(1));
        mapDriverUniqueMapper.withInput(new Text("jumps"), new IntWritable(1));

        mapDriverUniqueMapper.withOutput(new Text("the"), new IntWritable(1));
        mapDriverUniqueMapper.withOutput(new Text("quick"), new IntWritable(1));
        mapDriverUniqueMapper.withOutput(new Text("brown"), new IntWritable(1));
        mapDriverUniqueMapper.withOutput(new Text("fox"), new IntWritable(1));
        mapDriverUniqueMapper.withOutput(new Text("jumps"), new IntWritable(1));
        mapDriverUniqueMapper.runTest();
        System.out.println("Unique Mapper Unit Test Cases Success");
    }

    @Test
    public void testWordLengthMapper() throws IOException {
        mapDriverWordLength.withInput(new Text("the"), new IntWritable(1));
        mapDriverWordLength.withInput(new Text("quick"), new IntWritable(1));
        mapDriverWordLength.withInput(new Text("brown"), new IntWritable(1));
        mapDriverWordLength.withInput(new Text("fox"), new IntWritable(1));
        mapDriverWordLength.withInput(new Text("jumps"), new IntWritable(1));

        mapDriverWordLength.withOutput(new Text("3"), new IntWritable(1));
        mapDriverWordLength.withOutput(new Text("5"), new IntWritable(1));
        mapDriverWordLength.withOutput(new Text("5"), new IntWritable(1));
        mapDriverWordLength.withOutput(new Text("3"), new IntWritable(1));
        mapDriverWordLength.withOutput(new Text("5"), new IntWritable(1));
        mapDriverWordLength.runTest();
        System.out.println("Word Length Mapper Unit Test Cases Success");
    }

    private List<IntWritable> getTestData(int count) {
        List<IntWritable> values = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            values.add(new IntWritable(1));
        }
        return values;
    }

    @Test
    public void testIntSumReducer() throws IOException {
        reduceDriverIntSum.withInput(new Text("3"), getTestData(2));

        reduceDriverIntSum.withInput(new Text("5"), getTestData(4));

        reduceDriverIntSum.withOutput(new Text("3"), new IntWritable(2));
        reduceDriverIntSum.withOutput(new Text("5"), new IntWritable(4));

        reduceDriverIntSum.runTest();
        System.out.println("Int Sum Reducer Unit Test Cases Success");
    }

}
