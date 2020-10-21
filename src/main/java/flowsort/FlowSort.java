package flowsort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowSort {
    static class FlowSortMapper extends Mapper<LongWritable, Text, FlowBean, Text>{
        FlowBean flowBean = new FlowBean();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //拿到的是上一个汇总统计程序的输出的结果，已经是各手机号的总流量信息
            String line = value.toString();
            String[] fields = line.split("\t");
            String phone_num = fields[0];
            Long up_flow = Long.parseLong(fields[1].split(", ")[0].split("\\{")[1].split("=")[1]);
            Long d_flow = Long.parseLong(fields[1].split(", ")[1].split("=")[1]);
            flowBean.set(up_flow, d_flow);
            context.write(flowBean, new Text(phone_num));
        }
    }
    static class FlowSortReducer extends Reducer<FlowBean, Text, Text, FlowBean>{
        @Override
        protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text value: values){
                context.write(value, key);
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        //hadoop yarn模式的配置
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("fs.defaultFS", "hdfs://hadoop1:9000/");
        conf.set("yarn.resourcemanager.hostname", "hadoop1");

        //hadoop local模式的配置
//        conf.set("mapreduce.framework.name", "local");
//        conf.set("fs.defaultFS", "file:///");
        Job job = Job.getInstance(conf);

        //指定本程序的jar包所在的本地路径
        job.setJarByClass(FlowSort.class);

        //指定本业务job要使用的mapper业务类/指定本业务job要使用的reducer业务类
        job.setMapperClass(FlowSortMapper.class);
        job.setReducerClass(FlowSortReducer.class);

        //指定mapper输出的kv类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        //指定最终输出的kv类型
        job.setOutputKeyClass(FlowBean.class);
        job.setOutputValueClass(Text.class);
        //指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        //指定job输出结果所在目录
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //将job中配置的先关参数以及job所用的java类所在的jar包，提交给yarn运行
        /*job.submit();*/
        Thread.sleep(5000);
        boolean res = job.waitForCompletion(true);
        System.out.println(res);
        System.exit(res?0:1);
    }
}
