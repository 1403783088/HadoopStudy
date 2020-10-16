package provinceflow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

/**
 * K2 V2对应的是map输出对应的K V类型
 */
public class ProvincePartitioner extends Partitioner<Text, FlowBean> {
    public static HashMap<String, Integer> provinceDict = new HashMap<String, Integer>();
    static{
        provinceDict.put("136", 0);
        provinceDict.put("137", 1);
        provinceDict.put("138", 2);
        provinceDict.put("139", 3);

    }

    public int getPartition(Text key, FlowBean value, int numPartitions) {
        Integer provinceId = provinceDict.get(key.toString().substring(0,3));
        return (provinceId==null)?4:provinceId;
    }
}
