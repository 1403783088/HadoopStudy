package flowsort;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements Writable, WritableComparable<FlowBean>{
    private long up_flow;
    private long d_flow;
    private long sum_flow;

    //反序列化时，需要反射调用空参构造函数，所以要显式定义一个
    public FlowBean(){}

    public FlowBean(long up_flow, long d_flow) {
        this.up_flow = up_flow;
        this.d_flow = d_flow;
        this.sum_flow = up_flow + d_flow;
    }

    public void set(long up_flow, long d_flow) {
        this.up_flow = up_flow;
        this.d_flow = d_flow;
        this.sum_flow = up_flow + d_flow;
    }

    public long getUp_flow() {
        return up_flow;
    }

    public void setUp_flow(long up_flow) {
        this.up_flow = up_flow;
    }

    public long getD_flow() {
        return d_flow;
    }

    public void setD_flow(long d_flow) {
        this.d_flow = d_flow;
    }

    public long getSum_flow() {
        return sum_flow;
    }

    public void setSum_flow(long sum_flow) {
        this.sum_flow = sum_flow;
    }

    @Override
    public String toString() {
        return "FlowBean{" +
                "up_flow=" + up_flow +
                ", d_flow=" + d_flow +
                ", sum_flow=" + sum_flow +
                '}';
    }

    /**
     * 序列化方法
     * @param out
     * @throws IOException
     */
    public void write(DataOutput out) throws IOException {
        out.writeLong(up_flow);
        out.writeLong(d_flow);
        out.writeLong(sum_flow);
    }

    /**
     * 反序列化方法
     * 注意：反序列化的顺序跟序列化的顺序完全一致
     * @param in
     * @throws IOException
     */
    public void readFields(DataInput in) throws IOException {
        this.up_flow = in.readLong();
        this.d_flow = in.readLong();
        this.sum_flow = in.readLong();
    }


    public int compareTo(FlowBean otherFlowBean) {
        return this.sum_flow > otherFlowBean.sum_flow ? -1 : 1;
    }
}
