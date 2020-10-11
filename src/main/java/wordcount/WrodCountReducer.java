package wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * KEYIN, VALUEIN对应mapper输出的KEYOUT，VALUEOUT
 * KEYOUT，VALUEOUT是自定义reduce逻辑处理结果输出数据类型
 * KEYOUT是单词，TEXT
 * VALUEOUT是总次数，IntWritable
 */
public class WrodCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    /**
     *
     * @param key：是一组相同单词key-value对的key，<hello,1><hello,1><hello,1><hello,1><hello,1>中的第一个hello
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable value: values) {
            count += value.get();
        }
        context.write(key, new IntWritable(count));
    }
}
















