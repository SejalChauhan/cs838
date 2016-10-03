package org.myorg;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class AnagramSorter{
    public static class nn {}
    public class Anagram{
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{
        private final static Intwritable one = new IntWritable(1);
        private Text word = new text();


        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException{
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while(tokenier.hasMoreTokens()){
                word.set(tokenizer.nextToken());
                output.collect(word, one);
            }
        }
    }
    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>{
        public void reduce(Text key, iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException{
            int sum = 0;
            while(values.hasNext()){
                sum += values.next().get();
            }
            output.collect(key, new IntWritable(sum));
        }
	}

	public class Sorter{
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{
        private final static Intwritable one = new IntWritable(1);
        private Text word = new text();


        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException{
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while(tokenier.hasMoreTokens()){
                word.set(tokenizer.nextToken());
                output.collect(word, one);
            }
        }
    }
    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>{
        public void reduce(Text key, iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException{
            int sum = 0;
            while(values.hasNext()){
                sum += values.next().get();
            }
            output.collect(key, new IntWritable(sum));
        }
	}

    }
    public static void main(String args[]) throws Exception{
        JobConf conf1 = new JobConf(WordCount.class);
        conf1.setJobName("anagram");
	conf2.setJobName("sorter");

        conf1.setOutputKeyClass(Text.class);
        conf1.setOutputValueClass(IntWritable.class);

        conf1.setMapperClass(Map.class);
        conf1.setCombinerClass(Reduce.class);
        conf1.setReducerClass(Reduce.class);

        conf1.setInputFormat(TextInputFormat.class);
        conf1.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf1, new Path(arg[0]));
        FileOutputFormat.setOutputPaths(conf1, new Path(arg[1]));

        conf2.setOutputKeyClass(Text.class);
        conf2.setOutputValueClass(IntWritable.class);

        conf2.setMapperClass(Map.class);
        conf2.setCombinerClass(Reduce.class);
        conf2.setReducerClass(Reduce.class);

        conf2.setInputFormat(TextInputFormat.class);
        conf2.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf2, new Path(arg[0]));
        FileOutputFormat.setOutputPaths(conf2, new Path(arg[1]));

        JobClient.runJob(conf1);
	JobClient.runJob(conf2);
    }
}
