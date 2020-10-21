package rjoin;

import flowsum.FlowBean;
import flowsum.FlowSum;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

public class RJoin {
    static class RJoinMapper extends Mapper<LongWritable, Text, Text, InfoBean>{
        InfoBean infoBean = new InfoBean();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            String name = inputSplit.getPath().getName();
            String[] fields = value.toString().split("\t");
            String pid = "";
            //通过文件名判断是哪个文件中的数据
            if(name.startsWith("order")){
                pid = fields[2];
                infoBean.set(Integer.parseInt(fields[0]), fields[1], pid, Integer.parseInt(fields[3]), "", "", 0, "0");
            }else{
                pid = fields[0];
                infoBean.set(0, "", pid, 0, fields[1], fields[2], Integer.parseInt(fields[3]), "1");
            }
            context.write(new Text(pid), infoBean);
        }
    }
    static class RJoinReducer extends Reducer<Text, InfoBean, InfoBean, NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<InfoBean> values, Context context) throws IOException, InterruptedException {
            ArrayList<InfoBean> order_bean_list = new ArrayList<InfoBean>();
            ArrayList<InfoBean> product_bean_list = new ArrayList<InfoBean>();
            for (InfoBean value : values) {
                InfoBean infoBean = new InfoBean(value);
                if (infoBean.getFlag().equals("0")){
                    order_bean_list.add(infoBean);
                }else {
                    product_bean_list.add(infoBean);
                }
            }

            //拼接两类数据形成最终结果
            for (InfoBean order_info_bean:order_bean_list) {
                for (InfoBean product_info_bean:product_bean_list) {
                    order_info_bean.setPname(product_info_bean.getPname());
                    order_info_bean.setCategory_id(product_info_bean.getCategory_id());
                    order_info_bean.setPrice(product_info_bean.getPrice());
                    context.write(order_info_bean, null);
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        //hadoop yarn模式的配置
//        conf.set("mapreduce.framework.name", "yarn");
//        conf.set("fs.defaultFS", "hdfs://hadoop1:9000/");
//        conf.set("yarn.resourcemanager.hostname", "hadoop1");

        //hadoop local模式的配置
        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "file:///");
        Job job = Job.getInstance(conf);

        //指定本程序的jar包所在的本地路径
        job.setJarByClass(FlowSum.class);

        //指定本业务job要使用的mapper业务类/指定本业务job要使用的reducer业务类
        job.setMapperClass(RJoin.RJoinMapper.class);
        job.setReducerClass(RJoin.RJoinReducer.class);

        //指定mapper输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(InfoBean.class);

        //指定最终输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(InfoBean.class);
        //指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        //指定job输出结果所在目录
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //将job中配置的先关参数以及job所用的java类所在的jar包，提交给yarn运行
        /*job.submit();*/
        boolean res = job.waitForCompletion(true);
        System.out.println(res);
        System.exit(res?0:1);
    }
}
