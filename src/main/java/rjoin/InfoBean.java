package rjoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class InfoBean implements Writable {

    private int order_id;
    private String dateString;
    private String p_id;
    private int amount;
    private String pname;
    private String category_id;
    private float price;

    //flag=0表示这个对象封装的是订单表记录
    //flag=1表示这个对象封装的是产品信息记录
    private String flag;

    public InfoBean() { }

    public InfoBean(InfoBean infoBean_tmp){
        this.set(infoBean_tmp.getOrder_id(), infoBean_tmp.getDateString(), infoBean_tmp.getP_id(), infoBean_tmp.getAmount(), infoBean_tmp.getPname(), infoBean_tmp.getCategory_id(),infoBean_tmp.getPrice(), infoBean_tmp.getFlag());
    }

    public void set(int order_id, String dateString, String p_id, int amount, String pname, String category_id, float price, String flag) {
        this.order_id = order_id;
        this.dateString = dateString;
        this.p_id = p_id;
        this.amount = amount;
        this.pname = pname;
        this.category_id = category_id;
        this.price = price;
        this.flag = flag;
    }

    public void setOrder_id(int order_id) {
        this.order_id = order_id;
    }

    public void setDateString(String dateString) {
        this.dateString = dateString;
    }

    public void setP_id(String p_id) {
        this.p_id = p_id;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    public void setCategory_id(String category_id) {
        this.category_id = category_id;
    }

    public void setPrice(float price) {
        this.price = price;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    public int getOrder_id() {
        return order_id;
    }

    public String getDateString() {
        return dateString;
    }

    public String getP_id() {
        return p_id;
    }

    public int getAmount() {
        return amount;
    }

    public String getPname() {
        return pname;
    }

    public String getCategory_id() {
        return category_id;
    }

    public float getPrice() {
        return price;
    }

    public String getFlag() {
        return flag;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(order_id);
        out.writeUTF(dateString);
        out.writeUTF(p_id);
        out.writeInt(amount);
        out.writeUTF(pname);
        out.writeUTF(category_id);
        out.writeFloat(price);
        out.writeUTF(flag);
    }

    public void readFields(DataInput in) throws IOException {
        this.order_id = in.readInt();
        this.dateString = in.readUTF();
        this.p_id = in.readUTF();
        this.amount = in.readInt();
        this.pname = in.readUTF();
        this.category_id = in.readUTF();
        this.price = in.readFloat();
        this.flag = in.readUTF();
    }

    @Override
    public String toString() {
        return "order_id=" + order_id +
                ", dateString='" + dateString + '\'' +
                ", p_id=" + p_id +
                ", amount=" + amount +
                ", pname='" + pname + '\'' +
                ", category_id=" + category_id +
                ", price=" + price +
                ", flag='" + flag;
    }
}
