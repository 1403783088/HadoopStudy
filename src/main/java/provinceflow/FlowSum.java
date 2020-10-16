package provinceflow;

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

public class FlowSum {
//    private static final Logger Log = LoggerFactory.getLogger(FlowSum.class);

    static class FlowSumMapper extends Mapper<LongWritable, Text, Text, FlowBean> {


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //将一行内容转成String
            String line = value.toString();
            //切分字段
            String[] fields = line.split("\t");
            //取出手机号
            String phone_number = fields[1];
            //取出上行流量，下行流量
            long up_flow = Long.parseLong(fields[fields.length-3]);
            long d_flow = Long.parseLong(fields[fields.length-2]);
            FlowBean flowBean = new FlowBean(up_flow, d_flow);
            context.write(new Text(phone_number), flowBean);
        }
    }
    static class FlowSumReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
        //<18323, bean1><18323, bean2><18323, bean3><18323, bean4><18323, bean5>......
        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
            long sum_up_flow = 0;
            long sum_d_flow = 0;
            //遍历所有bean，将其中的上行流量，下行流量分别累加
            for (FlowBean bean: values) {
//                Log.info("up_flow: "+bean.getUp_flow());
                sum_up_flow += bean.getUp_flow();
                sum_d_flow += bean.getD_flow();
            }
            FlowBean result_bean = new FlowBean(sum_up_flow, sum_d_flow);
            context.write(key, result_bean);
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resourcemanager.hostname", "hadoop1");
        Job job = Job.getInstance(conf);

        //指定本程序的jar包所在的本地路径
        job.setJarByClass(FlowSum.class);

        //指定本业务job要使用的mapper业务类/指定本业务job要使用的reducer业务类
        job.setMapperClass(FlowSumMapper.class);
        job.setReducerClass(FlowSumReducer.class);

        //指定mapper输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        //同时指定相应“分区”数量的ReduceTask
        job.setMapOutputValueClass(FlowBean.class);

        //指定我们自定义的数据分区器
        job.setPartitionerClass(ProvincePartitioner.class);
        job.setNumReduceTasks(5);

        //指定最终输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
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
