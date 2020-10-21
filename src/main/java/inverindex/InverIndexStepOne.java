package inverindex;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class InverIndexStepOne {

    static class InverIndexStepOneMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        Text k = new Text();
        IntWritable v = new IntWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            String[] words = line.split(" ");

            FileSplit inputSplit = (FileSplit) context.getInputSplit();

            String file_name = inputSplit.getPath().getName();

            for(String word: words){
                k.set(word + "--" + file_name);
                context.write(k, v);
            }
        }
    }

    static class InverIndexStepOneReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        IntWritable value = new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            long count = 0;

            for(IntWritable value: values){
                count++;
            }

            context.write(key, value);
        }
    }
}
